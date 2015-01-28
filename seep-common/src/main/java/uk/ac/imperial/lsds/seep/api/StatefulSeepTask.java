package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.state.SeepState;

public interface StatefulSeepTask<K extends SeepState> extends SeepTask {

	public void setState(K state);
	
}
