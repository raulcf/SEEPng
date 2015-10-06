package uk.ac.imperial.lsds.seep.api.operator;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.Schema;

public class UpstreamConnection {

	private Operator upstreamOperator;
	private int streamId;
	private DataStore dataStore;
	private ConnectionType connectionType;
	
	public UpstreamConnection(Operator lo, int streamId, DataStore dataStore, ConnectionType connectionType) {
		this.upstreamOperator = lo;
		this.streamId = streamId;
		this.dataStore = dataStore;
		this.connectionType = connectionType;
	}
	
	public UpstreamConnection() { }

	public Operator getUpstreamOperator() {
		return upstreamOperator;
	}

	public ConnectionType getConnectionType() {
		return connectionType;
	}

	public int getStreamId() {
		return streamId;
	}

	public Schema getExpectedSchema(){
		return dataStore.getSchema();
	}

	public DataStore getDataStore(){
		return dataStore;
	}
	
	public DataStoreType getDataStoreType(){
		return dataStore.type();
	}
}
