package uk.ac.imperial.lsds.seep.api;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.errors.NotSupportedException;


public class FileSource implements Connectable, DataStoreDescriptor {

	private static LogicalOperator lo;
	private Properties config;
	
	private FileSource(int opId, Properties config){
		this.config = config;
		QueryBuilder qb = new QueryBuilder();
		lo = qb.newStatelessSource(new FileSourceImpl(), opId);
	}
	
	public static FileSource newSource(int opId, Properties config){
		return new FileSource(opId, config);
	}
	
	/** Implement DataOriginDescriptor **/
	
	@Override
	public DataStoreType type() {
		return DataStoreType.FILE;
	}
	
	/** Implement Connetable **/
	
	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema){
		DataStore dO = new DataStore(DataStoreType.FILE, config);
		lo.connectTo(downstreamOperator, streamId, schema, ConnectionType.ONE_AT_A_TIME, dO);
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, DataStore dO){
		throw new NotSupportedException("Cannot override DataOrigin");
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType conType){
		DataStore dO = new DataStore(DataStoreType.FILE, config);
		lo.connectTo(downstreamOperator, streamId, schema, conType, dO);
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType, DataStore dSrc) {
		throw new NotSupportedException("Cannot override DataOrigin");
	}
	
	private static class FileSourceImpl implements SeepTask, Source{
		@Override
		public void setUp() {		}
		@Override
		public void processData(ITuple data, API api) {		}
		@Override
		public void processDataGroup(ITuple dataBatch, API api) {		}
		@Override
		public void close() {		}
	}

	@Override
	public Properties getConfig() {
		// TODO Auto-generated method stub
		return null;
	}

}
