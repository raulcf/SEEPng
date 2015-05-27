package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.Schema;


public interface DataStoreDescriptor {
	
	public Schema getSchema();
	public DataStoreType type();
	public Properties getConfig();
	
}
