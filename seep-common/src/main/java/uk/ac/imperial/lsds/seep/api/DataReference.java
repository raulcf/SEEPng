package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public final class DataReference {

	private final DataStore dataStore;
	private final EndPoint endPoint;
	
	public DataReference(DataStore dataStore, EndPoint endPoint) {
		this.dataStore = dataStore;
		this.endPoint = endPoint;
	}
	
	public DataStore getDataStore() {
		return dataStore;
	}
	
	public EndPoint getEndPoint() {
		return endPoint;
	}
}
