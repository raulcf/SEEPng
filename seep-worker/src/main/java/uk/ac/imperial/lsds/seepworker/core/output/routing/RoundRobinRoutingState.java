package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.List;

public class RoundRobinRoutingState implements Router {

	private int totalRoutes;
	private int idx;
	private List<Integer> opIds; // TODO: Perhaps make this fixed and the class immutable?
	
	public RoundRobinRoutingState(List<Integer> opIds){
		this.totalRoutes = opIds.size();
		this.idx = 0;
		this.opIds = opIds;
	}
	
	@Override
	public int route(int key) {
		// TODO:
		return -1;
	}

	@Override
	public int route() {
		idx++;
		if(idx < 0){
			idx = 0;
		}
		int seekIdx = (idx)%totalRoutes;
		return opIds.get(seekIdx);
	}

}
