package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;


public interface DataStoreDescriptor {
	
	public DataStoreType type();
	public Properties getConfig();
	
}
