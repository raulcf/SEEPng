package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;

public class HdfsFileSource implements Connectable, DataStoreDescriptor {
	private static LogicalOperator lo;
	private Properties config;
	
	private HdfsFileSource(int opId, Properties config){
		this.config = config;
		QueryBuilder qb = new QueryBuilder();
		lo = qb.newStatelessSource(new HdfsFileSourceImpl(), opId);
	}
	public static HdfsFileSource newSource(int opId, Properties config){
		return new HdfsFileSource(opId, config);
	}
	@Override
	
	public DataStoreType type() {
		// TODO Auto-generated method stub
		return DataStoreType.HDFS;
	}

	@Override
	public Properties getConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,Schema schema) {
		DataStore dO = new DataStore(DataStoreType.HDFS, config);
		lo.connectTo(downstreamOperator, streamId, schema, ConnectionType.ONE_AT_A_TIME, dO);
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, ConnectionType connectionType) {
		// TODO Auto-generated method stub

	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, DataStore dSrc) {
		lo.connectTo(downstreamOperator, streamId, schema, ConnectionType.ONE_AT_A_TIME, dSrc);
		// TODO Auto-generated method stub

	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, ConnectionType connectionType, DataStore dSrc) {
		// TODO Auto-generated method stub

	}
	private static class HdfsFileSourceImpl implements SeepTask, Source{
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(ITuple dataBatch, API api) {		}
		@Override
		public void close() {		}
	}

}