package uk.ac.imperial.lsds.seep.api;

import java.util.List;
import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.state.SeepState;

public class HdfsSink implements Operator,DataStoreDescriptor{
	private static LogicalOperator lo;
	private Properties config;
	
	private HdfsSink(int opId,Properties p){
		this.config = p;
		QueryBuilder qb = new QueryBuilder();
		lo = qb.newStatelessSink(new HdfsSinkImpl(), opId);
	}
	public static HdfsSink newSink(int opId, Properties p){
		return new HdfsSink(opId,p);
	}
	public DataStore getDS(){
		return new DataStore(DataStoreType.HDFS,config);
	}
	public LogicalOperator getlo(){
		return lo;
	}
	private static class HdfsSinkImpl implements SeepTask,Sink{
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(ITuple dataBatch, API api) {		}
		@Override
		public void close() {		}
	}
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, ConnectionType connectionType) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, DataStore dSrc) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, ConnectionType connectionType, DataStore dSrc) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public DataStoreType type() {
		// TODO Auto-generated method stub
		return DataStoreType.HDFS;
	}
	@Override
	public Properties getConfig() {
		// TODO Auto-generated method stub
		return this.config;
	}
	@Override
	public int getOperatorId() {
		// TODO Auto-generated method stub
		return lo.getOperatorId();
	}
	@Override
	public String getOperatorName() {
		// TODO Auto-generated method stub
		return lo.getOperatorName();
	}
	@Override
	public boolean isStateful() {
		// TODO Auto-generated method stub
		return lo.isStateful();
	}
	@Override
	public SeepState getState() {
		// TODO Auto-generated method stub
		return lo.getState();
	}
	@Override
	public SeepTask getSeepTask() {
		// TODO Auto-generated method stub
		return lo.getSeepTask();
	}
	@Override
	public List<DownstreamConnection> downstreamConnections() {
		// TODO Auto-generated method stub
		return lo.downstreamConnections();
	}
	@Override
	public List<UpstreamConnection> upstreamConnections() {
		// TODO Auto-generated method stub
		return lo.upstreamConnections();
	}

}
