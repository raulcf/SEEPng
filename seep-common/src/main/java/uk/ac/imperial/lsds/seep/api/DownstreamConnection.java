package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.data.Schema;

public class DownstreamConnection {

	private Operator downstreamOperator;
	private int streamId;
	private Schema schema;
	private ConnectionType connectionType;
	private DataOrigin dSrc;

	public Operator getDownstreamOperator() {
		return downstreamOperator;
	}

	public void setDownstreamOperator(Operator downstreamOperator) {
		this.downstreamOperator = downstreamOperator;
	}

	public int getStreamId() {
		return streamId;
	}

	public void setStreamId(int streamId) {
		this.streamId = streamId;
	}
	
	public Schema getSchema(){
		return schema;
	}
	
	public void setSchema(Schema schema){
		this.schema = schema;
	}
	
	public ConnectionType getConnectionType(){
		return connectionType;
	}
	
	public void setConnectionType(ConnectionType connectionType){
		this.connectionType = connectionType;
	}
	
	public DataOriginType getExpectedDataOriginTypeOfDownstream(){
		return dSrc.type();
	}
	
	public DataOrigin getExpectedDataOriginOfDownstream(){
		return dSrc;
	}
	
	public void setExpectedDataOriginOfDownstream(DataOrigin dSrc){
		this.dSrc = dSrc;
	}
	
	public DownstreamConnection(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType, DataOrigin dSrc){
		this.setDownstreamOperator(downstreamOperator);
		this.setStreamId(streamId);
		this.setSchema(schema);
		this.setConnectionType(connectionType);
		this.setExpectedDataOriginOfDownstream(dSrc);
	}
	
	public void replaceOperator(Operator replacement){
		this.downstreamOperator = replacement;
	}
	
}
