package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.data.Schema;

public class UpstreamConnection {

	private Operator upstreamOperator;
	private ConnectionType connectionType;
	private int streamId;
	private Schema schema;
	private DataStore dSrc;

	public Operator getUpstreamOperator() {
		return upstreamOperator;
	}

	public void setUpstreamOperator(Operator upstreamOperator) {
		this.upstreamOperator = upstreamOperator;
	}

	public ConnectionType getConnectionType() {
		return connectionType;
	}

	public void setConnectionType(ConnectionType connectionType) {
		this.connectionType = connectionType;
	}

	public int getStreamId() {
		return streamId;
	}

	public void setStreamId(int streamId) {
		this.streamId = streamId;
	}
	
	public Schema getExpectedSchema(){
		return schema;
	}
	
	public void setExpectedSchema(Schema schema){
		this.schema = schema;
	}
	
	public DataStore getDataOrigin(){
		return dSrc;
	}
	
	public DataStoreType getDataOriginType(){
		return dSrc.type();
	}
	
	public void setDataOrigin(DataStore dSrc){
		this.dSrc = dSrc;
	}
	
	public UpstreamConnection(Operator upstreamOperator, ConnectionType connectionType, int streamId, Schema schema, DataStore dSrc){
		this.setUpstreamOperator(upstreamOperator);
		this.setConnectionType(connectionType);
		this.setStreamId(streamId);
		this.setExpectedSchema(schema);
		this.setDataOrigin(dSrc);
	}
	
	public void replaceOperator(Operator upstreamOperator) {
		this.upstreamOperator = upstreamOperator;
	}
	
}
