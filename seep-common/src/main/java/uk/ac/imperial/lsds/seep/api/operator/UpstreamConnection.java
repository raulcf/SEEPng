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
	
//	public UpstreamConnection(Operator upstreamOperator, ConnectionType connectionType, int streamId, Schema schema, DataStore dSrc){
//		this.setUpstreamOperator(upstreamOperator);
//		this.setConnectionType(connectionType);
//		this.setStreamId(streamId);
//		this.setExpectedSchema(schema);
//		this.setDataOrigin(dSrc);
//	}
	
	public UpstreamConnection(Operator lo, int streamId, DataStore dataStore, ConnectionType connectionType) {
		this.upstreamOperator = lo;
		this.streamId = streamId;
		this.dataStore = dataStore;
		this.connectionType = connectionType;
	}

	public Operator getUpstreamOperator() {
		return upstreamOperator;
	}

//	public void setUpstreamOperator(Operator upstreamOperator) {
//		this.upstreamOperator = upstreamOperator;
//	}

	public ConnectionType getConnectionType() {
		return connectionType;
	}

//	public void setConnectionType(ConnectionType connectionType) {
//		this.connectionType = connectionType;
//	}

	public int getStreamId() {
		return streamId;
	}

//	public void setStreamId(int streamId) {
//		this.streamId = streamId;
//	}
	
	public Schema getExpectedSchema(){
		return dataStore.getSchema();
	}
	
//	public void setExpectedSchema(Schema schema){
//		this.schema = schema;
//	}
	
	public DataStore getDataStore(){
		return dataStore;
	}
	
	public DataStoreType getDataStoreType(){
		return dataStore.type();
	}
	
//	public void setDataOrigin(DataStore dSrc){
//		this.dataStore = dSrc;
//	}

//	public void replaceOperator(Operator upstreamOperator) {
//		this.upstreamOperator = upstreamOperator;
//	}
	
}
