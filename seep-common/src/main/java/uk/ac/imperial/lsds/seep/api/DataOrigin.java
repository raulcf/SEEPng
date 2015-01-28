package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;

public class DataOrigin implements DataOriginDescriptor{

	private DataOriginType type;
	private String resource;
	private SerializerType serde;
		
	public DataOrigin(DataOriginType type, String resource, SerializerType serde){
		this.type = type;
		this.resource = resource;
		this.serde = serde;
	}
		
	@Override
	public DataOriginType type(){
		return type;
	}
	
	@Override
	public String getResourceDescriptor() {
		return resource;
	}

	@Override
	public SerializerType getSerdeType() {
		return serde;
	}

}
