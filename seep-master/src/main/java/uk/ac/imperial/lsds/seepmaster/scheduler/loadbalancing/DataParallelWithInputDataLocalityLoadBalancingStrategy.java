package uk.ac.imperial.lsds.seepmaster.scheduler.loadbalancing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.SeepCommand;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.ClusterDatasetRegistry;
import uk.ac.imperial.lsds.seepmaster.scheduler.CommandToNode;
import uk.ac.imperial.lsds.seepmaster.scheduler.ScheduleTracker;

public class DataParallelWithInputDataLocalityLoadBalancingStrategy implements LoadBalancingStrategy {

	@Override
	public List<CommandToNode> assignWorkToWorkers(Stage nextStage, InfrastructureManager inf, ScheduleTracker tracker) {
		// moved in from previously external method
		Set<Connection> conns = getWorkersInvolvedInStage(nextStage, inf);
		
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
						else if(dr.getControlEndPoint().getId() == c.getId()) {
							// assign
							assignDataReferenceToWorker(perWorker, streamId, dr);
						}
					}
				}
				currentWorker++;
			}
			// FIXME: what is outputdatareferences
			int euId = c.getId();
			List<Integer> rankedDatasets = tracker.getClusterDatasetRegistry().getRankedDatasetForNode(euId, tracker.getScheduleDescription());
			esc = ProtocolCommandFactory.buildScheduleStageCommand(nextStageId, 
					perWorker, nextStage.getOutputDataReferences(), rankedDatasets);
			CommandToNode ctn = new CommandToNode(esc, c);
			commands.add(ctn);
		}
		return commands;
	}
	
	private Set<Connection> getWorkersInvolvedInStage(Stage stage, InfrastructureManager inf) {
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
	
	private void assignDataReferenceToWorker(Map<Integer, Set<DataReference>> perWorker, int streamId, DataReference dr) {
		if(! perWorker.containsKey(streamId)) {
			perWorker.put(streamId, new HashSet<>());
		}
		perWorker.get(streamId).add(dr);
	}

}
