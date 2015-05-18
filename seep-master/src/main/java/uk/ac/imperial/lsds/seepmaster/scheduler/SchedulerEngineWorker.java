package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

import com.esotericsoftware.kryo.Kryo;

public class SchedulerEngineWorker implements Runnable {

	final private Logger LOG = LoggerFactory.getLogger(SchedulerEngineWorker.class);
	
	private ScheduleDescription scheduleDescription;
	private SchedulingStrategy schedulingStrategy;
	private ScheduleTracker tracker;
	
	private InfrastructureManager inf;
	private Set<Connection> connections;
	private Comm comm;
	private Kryo k;
	
	private boolean work = true;
	
	public SchedulerEngineWorker(ScheduleDescription sdesc, SchedulingStrategy schedulingStrategy, InfrastructureManager inf, Comm comm, Kryo k) {
		this.scheduleDescription = sdesc;
		this.schedulingStrategy = schedulingStrategy;
		this.tracker = new ScheduleTracker(scheduleDescription.getStages());
		this.inf = inf;
		this.comm = comm;
		this.k = k;
	}

	public void stop() {
		this.work = false;
	}
	
	@Override
	public void run() {
		while(work) {
			// Get next stage
			Stage nextStage = schedulingStrategy.next(tracker);
			if(nextStage == null) {
				// TODO: means the computation finished, do something
			}
			// TODO: Come up with a plan: which workers process which dataReferences and where they output data
			MasterWorkerCommand esc = ProtocolCommandFactory.buildScheduleStageCommand(nextStage.getStageId(), 
					nextStage.getInputDataReferences(), nextStage.getOutputDataReferences());
			Set<Connection> euInvolved = getWorkersInvolvedInStage(nextStage);
			// Send stage to all workers and wait... (easy to get only a subset, since we have inf here)
			boolean success = comm.send_object_sync(esc, euInvolved, k);
			// Wait until stage is completed
			waitForNodes(nextStage, euInvolved);
		}
	}
	
	private Set<Connection> getWorkersInvolvedInStage(Stage stage) {
		Set<EndPoint> eps = stage.getInvolvedNodes();
		Set<Connection> cons = new HashSet<>();
		for(EndPoint ep : eps) {
			Connection c = new Connection(ep);
			cons.add(c);
		}
		return cons;
	}
	
	private void waitForNodes(Stage stage, Set<Connection> euInvolved) {
		// better to wait on a latch or something....
		Set<Integer> euIds = new HashSet<>();
		for(Connection c : euInvolved) {
			euIds.add(c.getId());
		}
		tracker.trackAndWait(stage, euIds);
	}
	
	public boolean prepareForStart(Set<Connection> connections) {
		// Set initial connections in worker
		this.connections = connections;
		// Basically change stage status so that SOURCE tasks are ready to run
		boolean success = true;
		for(Stage stage : scheduleDescription.getStages()) {
			if(stage.getStageType().equals(StageType.UNIQUE_STAGE) || stage.getStageType().equals(StageType.SOURCE_STAGE)) {
				// TODO: configure inputDataReference at this point ??
				boolean changed = tracker.setReady(stage);
				success = success && changed;
			}
		}
		return success;
	}
	
	public void newStageStatus(int stageId, int euId, Map<Integer, Set<DataReference>> results, StageStatusCommand.Status status) {
		switch(status) {
		case OK:
			LOG.info("EU {} finishes stage {}", euId, stageId);
			tracker.finishStage(euId, stageId, results);
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
		return schedulingStrategy.next(tracker);
	}
	
	public void __reset_schedule() {
		tracker.resetAllStagesTo(StageStatus.WAITING);
	}
}
