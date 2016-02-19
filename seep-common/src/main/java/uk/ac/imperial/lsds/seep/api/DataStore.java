package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.Schema;

public class DataStore implements DataStoreDescriptor {

	private Schema dataSchema;
	private DataStoreType type;
	private Properties config;
	
	public DataStore(DataStoreType emptyType) {
		if(emptyType != DataStoreType.EMPTY) {
			// TODO: throw exception, no schema can only mean no input args
		}
		this.type = emptyType;
	}
	
	public DataStore(Schema schema, DataStoreType type) {
		this.dataSchema = schema;
		this.type = type;
	}
	
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
	
	/**
	 * Empty constructor for kryo serialization
	 */
	public DataStore() { }

}
