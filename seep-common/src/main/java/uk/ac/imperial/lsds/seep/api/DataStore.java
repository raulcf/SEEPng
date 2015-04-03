package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.config.Config;

public class DataStore implements DataStoreDescriptor {

	private DataStoreType type;
	private Config config;
		
	public DataStore(DataStoreType type, Config config){
		this.type = type;
		this.config = config;
	}

	@Override
	public DataStoreType type(){
		return type;
	}

	@Override
	public Config getConfig(){
		return config;
	}

}
