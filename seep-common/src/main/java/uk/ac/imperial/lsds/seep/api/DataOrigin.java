package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;

public class DataOrigin implements DataOriginDescriptor{

	private DataOriginType type;
	private String resource;
	private SerializerType serde;
	private Schema schema;
	private Properties properties;
		
	public DataOrigin(DataOriginType type, String resource, SerializerType serde, Schema schema, Properties prop){
		this.type = type;
		this.resource = resource;
		this.serde = serde;
		this.schema = schema;
		this.properties = prop;
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
	public Schema getSchema(){
		return schema;
	}
	
	@Override
	public Properties getProperties(){
		return properties;
	}

}
