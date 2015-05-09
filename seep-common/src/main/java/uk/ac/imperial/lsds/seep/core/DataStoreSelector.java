package uk.ac.imperial.lsds.seep.core;

public interface DataStoreSelector {

	public boolean initSelector();
	public boolean startSelector();
	public boolean stopSelector();
	
}
