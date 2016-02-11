package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class DataParallelWithInputDataLocalityLoadBalancingStrategy implements LoadBalancingStrategy {

	@Override
	public List<CommandToNode> assignWorkToWorkers(Stage nextStage, Set<Connection> conns) {
		// All input data references to process during next stage
		int nextStageId = nextStage.getStageId();
		Map<Integer, Set<DataReference>> drefs = nextStage.getInputDataReferences();
		
		// Split input DataReference per worker to maximize locality (not load balancing)
		List<CommandToNode> commands = new ArrayList<>();
		final int totalWorkers = conns.size();
		int currentWorker = 0;
		for(Connection c : conns) {
			MasterWorkerCommand esc = null;
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
						else if(dr.getEndPoint().getId() == c.getId()) {
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

}
