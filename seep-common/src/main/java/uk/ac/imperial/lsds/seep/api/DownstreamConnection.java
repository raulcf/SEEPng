package uk.ac.imperial.lsds.seep.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.data.Schema;

public class DownstreamConnection {

	final private Logger LOG = LoggerFactory.getLogger(DownstreamConnection.class);
	
	private Operator downstreamOperator;
	private int streamId;
	private Schema schema;
	private ConnectionType connectionType;
	private DataStore dSrc;

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
	
	public DataStoreType getExpectedDataOriginTypeOfDownstream(){
		return dSrc.type();
	}
	
	public DataStore getExpectedDataOriginOfDownstream(){
		return dSrc;
	}
	
	public void setExpectedDataOriginOfDownstream(DataStore dSrc){
		this.dSrc = dSrc;
	}
	
	public DownstreamConnection(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType, DataStore dSrc){
		this.setDownstreamOperator(downstreamOperator);
		this.setStreamId(streamId);
		this.setSchema(schema);
		this.setConnectionType(connectionType);
		this.setExpectedDataOriginOfDownstream(dSrc);
	}
	
	public void replaceOperator(Operator replacement){
		LOG.trace("Replacing {} by {}", this.downstreamOperator, replacement);
		this.downstreamOperator = replacement;
	}
	
}
