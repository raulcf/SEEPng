package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;

public class RoundRobinRoutingState implements Router {

	private int totalRoutes;
	private int idx;
	private List<DownstreamConnection> cons;
	
	public RoundRobinRoutingState(List<DownstreamConnection> cons){
		this.totalRoutes = cons.size();
		this.idx = 0;
		this.cons = cons;
	}
	
	@Override
	public OutputBuffer route(Map<Integer, OutputBuffer> obufs, int key) {
		// TODO:
		return null;
	}

	@Override
	public OutputBuffer route(Map<Integer, OutputBuffer> obufs) {
		int seekIdx = (idx++)%totalRoutes;
		// TODO: check this thing is an arraylist for efficient indexed accesss
		int id = cons.get(seekIdx).getDownstreamOperator().getOperatorId();
		return obufs.get(id);
	}

}
