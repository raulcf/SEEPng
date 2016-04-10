package uk.ac.imperial.lsds.seepmaster.scheduler;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.Command;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.metrics.SeepMetrics;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

import com.codahale.metrics.Timer;
import com.esotericsoftware.kryo.Kryo;

public class SchedulerEngineWorker implements Runnable {

	final private Logger LOG = LoggerFactory.getLogger(SchedulerEngineWorker.class);
	
	private ScheduleDescription scheduleDescription;
	private SchedulingStrategy schedulingStrategy;
	private LoadBalancingStrategy loadBalancingStrategy;
	private ScheduleTracker tracker;
	
	private InfrastructureManager inf;
	private Set<Connection> connections;
	private Comm comm;
	private Kryo k;
	
	private boolean work = true;
	
	public SchedulerEngineWorker(ScheduleDescription sdesc, SchedulingStrategy schedulingStrategy, LoadBalancingStrategy loadBalancingStrategy, InfrastructureManager inf, Comm comm, Kryo k) {
		this.scheduleDescription = sdesc;
		this.schedulingStrategy = schedulingStrategy;
		this.loadBalancingStrategy = loadBalancingStrategy;
		this.tracker = new ScheduleTracker(scheduleDescription);
		this.inf = inf;
		this.comm = comm;
		this.k = k;
	}

	public void stop() {
		this.work = false;
	}
	
	@Override
	public void run() {
		LOG.info("[START JOB]");
		long scheduleStart = System.nanoTime();
		while(work) {
			
			// At the end of one iteration the worker will have populated the commands that need to be sent to the cluster
			// Some of these commands are schedule stage commands. Other are about evicting datasets, etc.
			List<CommandToNode> commands = new ArrayList<>();
			
			
			Map<Integer, List<RuntimeEvent>> rEvents = null;
			// Check whether the last executed stage generated runtime events that need to be handled here
			if(tracker.didLastStageGenerateRuntimeEvents()) {
				rEvents = tracker.getRuntimeEventsOfLastStageExecution();
				// CURRENT EVENTS:
				// OutOfMemory a dataset was spilled to disk, update any info that exists here about that
				// A loop was finished, bear that in mind to choose the next stage to schedule
			}
			// Get next stage
			// TODO: make next return a List of next stages
			Stage nextStage = schedulingStrategy.next(tracker, rEvents);
			if(nextStage == null) {
				// TODO: means the computation finished, do something
				if(tracker.isScheduledFinished()) {	
					long scheduleFinish = System.nanoTime();
					long totalScheduleTime = scheduleFinish - scheduleStart;
					LOG.info("[END JOB] !!! {}", totalScheduleTime);
					work = false;
					continue;
				}
			}
			
			// TODO: (parallel sched) make this receive a list of stages
			List<CommandToNode> schedCommands = loadBalancingStrategy.assignWorkToWorkers(nextStage, inf, tracker.getClusterDatasetRegistry());
			commands.addAll(schedCommands); // append scheduling commands to the commands necessary to send to the cluster
			
			// FIXME: avoid extracting conns here. They need to be extracted again immediately after
			// we should have a tracker entity that receives progressively what to track, and then we 
			// just pass info (the connections) to that guy
			long stageStart = System.nanoTime();
			Set<Connection> euInvolved = new HashSet<>();
			for(CommandToNode ctn : commands) {
				euInvolved.add(ctn.c);
			}
			
			// TODO: (parallel sched) adapt tracking structures to track multiple stages simultaneously
			trackStageCompletionAsync(nextStage, euInvolved);
			
			LOG.info("[START] SCHEDULING Stage {}", nextStage.getStageId());
			for(CommandToNode ctn : commands) {
				boolean success = comm.send_object_sync(ctn.command, ctn.c, k);
			}
			
			// TODO: make this compatible with waiting for multiple parallel schedule stages
			tracker.waitForFinishedStageAndCompleteBookeeping(nextStage);
			
			// Call the post processing event
			List<Command> postCommands = schedulingStrategy.postCompletion(nextStage, tracker);
			
			long stageFinish = System.nanoTime();
			long totalStageTime = stageFinish - stageStart;
			LOG.warn("Stage {} finished in: ! {}", nextStage.getStageId(), totalStageTime);
			
			if(! commands.isEmpty()) {
				// TODO:
			}
			
		}
	}
	
	private void trackStageCompletionAsync(Stage stage, Set<Connection> euInvolved) {
		// Just start the tracker async
		new Thread(new Runnable() {
			public void run() {
				// Wait until stage is completed
				Set<Integer> euIds = new HashSet<>();
				for(Connection c : euInvolved) {
					euIds.add(c.getId());
				}
				tracker.trackWorkersAndBlock(stage, euIds);
			}
		}).start();
	}
	
	public boolean prepareForStart(Set<Connection> connections) {
		// Set initial connections in worker
		this.connections = connections;
		// Basically change stage status so that SOURCE tasks are ready to run
		boolean success = true;
		for(Stage stage : scheduleDescription.getStages()) {
			if(stage.getStageType().equals(StageType.UNIQUE_STAGE) || stage.getStageType().equals(StageType.SOURCE_STAGE)) {
				configureInputForInitialStage(connections, stage, scheduleDescription);
				boolean changed = tracker.setReady(stage);
				success = success && changed;
			}
		}
		return success;
	}

	private void configureInputForInitialStage(Set<Connection> connections, Stage s, ScheduleDescription sd) {
		// Check whether the stage needs to be configured or whether it comes configured already
		// such as in the case of a handcrafted schedule
		if(! s.getInputDataReferences().isEmpty()) {
			// It's already configured
			return;
		}
		// Get input type from first operator
		int srcOpId = s.getWrappedOperators().getLast();
		LogicalOperator src = sd.getOperatorWithId(srcOpId);
		Set<DataReference> refs = new HashSet<>();
		
		// We need to get the DataStore to configure a DataReference
		DataStore dataStore = null;
		// We handle here the special case of having a marker source operator, in which case it dissapeared and is null
		for(UpstreamConnection uc : src.upstreamConnections()) {
			if (uc.getUpstreamOperator() == null) {
				dataStore = uc.getDataStore();
			}
		}
		// If dataStore was not set above, then there is a real source operator, that we set here
		if(dataStore == null) {
			dataStore = src.upstreamConnections().iterator().next().getUpstreamOperator().upstreamConnections().iterator().next().getDataStore();
		}
		
		int streamId = 0; // only one streamId for sources in scheduled mode	
		DataReference dr = DataReference.makeExternalDataReference(dataStore);
		refs.add(dr);
		s.addInputDataReference(streamId, refs);
	}
	
	public void newStageStatus(int stageId, int euId, 
			Map<Integer, Set<DataReference>> results, 
			StageStatusCommand.Status status,
			List<RuntimeEvent> runtimeEvents,
			Set<Integer> managedDatasets) {
		switch(status) {
		case OK:
			LOG.info("EU {} finishes stage {}", euId, stageId);
			tracker.finishStage(euId, stageId, results, runtimeEvents, managedDatasets);
			break;
		case FAIL:
			LOG.info("EU {} has failed executing stage {}", euId, stageId);
			
			break;
		default:
			
			LOG.error("Unrecognized STATUS in StageStatusCommand");
		}
	}
	
	/** Methods to facilitate testing **/
	
	public ScheduleTracker __tracker_for_testing(){
		return tracker;
	}
	
	public Stage __next_stage_scheduler(){
		return schedulingStrategy.next(tracker, null);
	}
	
	public void __reset_schedule() {
		tracker.resetAllStagesTo(StageStatus.WAITING);
	}
}
