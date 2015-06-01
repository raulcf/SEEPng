package uk.ac.imperial.lsds.seep.core;

import uk.ac.imperial.lsds.seep.api.DataStoreType;

public interface DataStoreSelector {

	public DataStoreType type();
	public boolean initSelector();
	public boolean startSelector();
	public boolean stopSelector();
		
}
