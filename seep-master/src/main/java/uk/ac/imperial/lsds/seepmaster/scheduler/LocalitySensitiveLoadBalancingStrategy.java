package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.SeepCommand;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

public class LocalitySensitiveLoadBalancingStrategy implements LoadBalancingStrategy {

	@Override
	public List<CommandToNode> assignWorkToWorkers(Stage stage, InfrastructureManager inf) {
		List<CommandToNode> ctns = new ArrayList<>();
		
		// Simply get locality information from Stage
		ControlEndPoint cep = stage.getStageLocation();
		Connection cToWorker = new Connection(cep);
		
		int stageId = stage.getStageId();
		Map<Integer, Set<DataReference>> input = stage.getInputDataReferences();
		Map<Integer, Set<DataReference>> output = stage.getOutputDataReferences();
		SeepCommand esc = ProtocolCommandFactory.buildScheduleStageCommand(stageId, 
				input, output);
		CommandToNode ctn = new CommandToNode(esc, cToWorker);
		ctns.add(ctn);
		return ctns;
	}

}
