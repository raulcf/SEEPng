package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.Schema;

public class DataStore implements DataStoreDescriptor {

	private Schema dataSchema;
	private DataStoreType type;
	private Properties config;
		
	public DataStore(Schema schema, DataStoreType type, Properties config){
		this.dataSchema = schema;
		this.type = type;
		this.config = config;
	}
	
	@Override
	public Schema getSchema(){
		return dataSchema;
	}

	@Override
	public DataStoreType type(){
		return type;
	}

	@Override
	public Properties getConfig(){
		return config;
	}

}
