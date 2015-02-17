package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;
import uk.ac.imperial.lsds.seep.config.Config;

public class DataOrigin implements DataOriginDescriptor{

	private DataOriginType type;
	@Deprecated
	private String resource;
	@Deprecated
	private SerializerType serde;
	// Config will incorporate (in the future) resource and serde
	private Config config;
		
	public DataOrigin(DataOriginType type, String resource, SerializerType serde, Config config){
		this.type = type;
		this.resource = resource;
		this.serde = serde;
		this.config = config;
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
	
	@Override
	public Config getConfig(){
		return config;
	}

}
