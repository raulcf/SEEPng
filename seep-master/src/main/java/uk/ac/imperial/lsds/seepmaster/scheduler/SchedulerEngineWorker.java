package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.SeepCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

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
		LOG.info("[START JOB]");
		while(work) {
			// Get next stage
			Stage nextStage = schedulingStrategy.next(tracker);
			if(nextStage == null) {
				// TODO: means the computation finished, do something
				if(tracker.isScheduledFinished()) {
					LOG.info("TODO: 1-Schedule has finished at this point");
					work = false;
					continue;
				}
			}
			
			Set<Connection> euInvolved = getWorkersInvolvedInStage(nextStage);
			
			trackStageCompletionAsync(nextStage, euInvolved);
			
			List<CommandToNode> commands = assignWorkToWorkers(nextStage, euInvolved);
			
			LOG.info("[START] SCHEDULING Stage {}", nextStage.getStageId());
			for(CommandToNode ctn : commands) {
				boolean success = comm.send_object_sync(ctn.command, ctn.c, k);
			}
			
			tracker.waitForFinishedStageAndCompleteBookeeping(nextStage);
		}
	}
	
	private class CommandToNode {
		public CommandToNode(SeepCommand command, Connection c){
			this.command = command;
			this.c = c;
		}
		public SeepCommand command;
		public Connection c;
	}
	
	// TODO: straw-man solution
	// TODO: this guy will receive info about each node status, so that it can make decisions on how each Dataref must be stored
	private List<CommandToNode> assignWorkToWorkers(Stage nextStage, Set<Connection> conns) {
		// All input data references to process during next stage
		int nextStageId = nextStage.getStageId();
		Map<Integer, Set<DataReference>> drefs = nextStage.getInputDataReferences();
		
		// Split input DataReference per worker to maximize locality (not load balancing)
		List<CommandToNode> commands = new ArrayList<>();
		final int totalWorkers = conns.size();
		int currentWorker = 0;
		for(Connection c : conns) {
			SeepCommand esc = null;
			Map<Integer, Set<DataReference>> perWorker = new HashMap<>();
			for(Integer streamId : drefs.keySet()) {
				for(DataReference dr : drefs.get(streamId)) {
					// EXTERNAL. assign one and continue
					if(! dr.isManaged()) {
						assignDataReferenceToWorker(perWorker, streamId, dr);
						currentWorker++;
						break;
					}
					// MANAGED. Check whether to assign this DR or not. Assign when shuffled or locality=local
					else {
						// SHUFFLE/PARTITIONED CASE
						if(dr.isPartitioned()) {
							// In this case, assign to this worker all DataReference with seqId module
							int partitionSeqId = dr.getPartitionId();
							if(partitionSeqId % totalWorkers == currentWorker) {
								assignDataReferenceToWorker(perWorker, streamId, dr);
							}
						}
						// NORMAL CASE, MAKE LOCALITY=LOCAL
						else if(dr.getDataEndPoint().getId() == c.getId()) {
							// assign
							assignDataReferenceToWorker(perWorker, streamId, dr);
						}
					}
				}
				currentWorker++;
			}
			// FIXME: what is outputdatareferences
			esc = ProtocolCommandFactory.buildScheduleStageCommand(nextStageId, 
					perWorker, nextStage.getOutputDataReferences());
			CommandToNode ctn = new CommandToNode(esc, c);
			commands.add(ctn);
		}
		return commands;
	}
	
	private void assignDataReferenceToWorker(Map<Integer, Set<DataReference>> perWorker, int streamId, DataReference dr) {
		if(! perWorker.containsKey(streamId)) {
			perWorker.put(streamId, new HashSet<>());
		}
		perWorker.get(streamId).add(dr);
	}
	
	private Set<Connection> getWorkersInvolvedInStage(Stage stage) {
		Set<Connection> cons = new HashSet<>();
		// In this case DataReference do not necessarily contain EndPoint information
		if(stage.getStageType().equals(StageType.SOURCE_STAGE) || stage.getStageType().equals(StageType.UNIQUE_STAGE)) {
			//TODO: probably this won't work later
			// Simply report all nodes
			for(ExecutionUnit eu : inf.executionUnitsInUse()) {
				Connection conn = new Connection(eu.getControlEndPoint());
				cons.add(conn);
			}
		}
		// If not first stages, then DataReferences contain the right EndPoint information
		else {
			Set<SeepEndPoint> eps = stage.getInvolvedNodes();
			for(SeepEndPoint ep : eps) {
				Connection c = new Connection(ep);
				cons.add(c);
			}
		}
		return cons;
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
	
	public boolean prepareForStart(Set<Connection> connections, SeepLogicalQuery slq) {
		// Set initial connections in worker
		this.connections = connections;
		// Basically change stage status so that SOURCE tasks are ready to run
		boolean success = true;
		for(Stage stage : scheduleDescription.getStages()) {
			if(stage.getStageType().equals(StageType.UNIQUE_STAGE) || stage.getStageType().equals(StageType.SOURCE_STAGE)) {
				configureInputForInitialStage(connections, stage, slq);
				boolean changed = tracker.setReady(stage);
				success = success && changed;
			}
		}
		return success;
	}

	private void configureInputForInitialStage(Set<Connection> connections, Stage s, SeepLogicalQuery slq) {
		// Get input type from first operator
		int srcOpId = s.getWrappedOperators().getLast();
		LogicalOperator src = slq.getOperatorWithId(srcOpId);
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
		
//		// Create dataReferences per connection
//		for(Connection c : connections) {
//			// When downstream requires partitioned DataReferences
//			if(s.hasDependantWithPartitionedStage()) {
//
//				int numPartitions = 8;
//				// TODO: create a DR per partition and assign the partitionSeqId
//				for(int i = 0; i < numPartitions; i++) {
//					// FIXME: EndPoint should disappear in favor of the safer SeepEndPoint
//					SeepEndPoint sep = c.getAssociatedEndPoint();
//					EndPoint endPoint = new EndPoint(sep.getId(), sep.getIp(), sep.getPort());
//					
//					DataReference dr = null;
//					int partitionId = i;
//					dr = DataReference.makeManagedAndPartitionedDataReference(dataStore, endPoint, ServeMode.STORE, partitionId);
//					refs.add(dr);
//				}
//			}
//			// When a normal data reference is required
//			else {
//				DataReference dr = DataReference.makeExternalDataReference(dataStore);
//				refs.add(dr);
//			}
//		}
		
		DataReference dr = DataReference.makeExternalDataReference(dataStore);
		refs.add(dr);
		s.addInputDataReference(streamId, refs);
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
