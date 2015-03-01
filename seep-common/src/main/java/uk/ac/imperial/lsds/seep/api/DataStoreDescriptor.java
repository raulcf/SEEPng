package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;
import uk.ac.imperial.lsds.seep.config.Config;

public interface DataStoreDescriptor {
	
	public DataStoreType type();
	public String getResourceDescriptor();
	public SerializerType getSerdeType();
	public Config getConfig();
	
}
