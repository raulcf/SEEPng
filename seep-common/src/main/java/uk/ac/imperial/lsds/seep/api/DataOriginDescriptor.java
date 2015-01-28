package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;

public interface DataOriginDescriptor {
	
	public DataOriginType type();
	public String getResourceDescriptor();
	public SerializerType getSerdeType();
	
}
