package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

public class DataStore implements DataStoreDescriptor {

	private DataStoreType type;
	private Properties config;
		
	public DataStore(DataStoreType type, Properties config){
		this.type = type;
		this.config = config;
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
