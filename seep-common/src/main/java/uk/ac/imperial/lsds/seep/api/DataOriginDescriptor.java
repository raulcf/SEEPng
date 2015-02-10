package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;

public interface DataOriginDescriptor {
	
	public DataOriginType type();
	public String getResourceDescriptor();
	public SerializerType getSerdeType();
	public Schema getSchema();
	public Properties getProperties();
	
}
