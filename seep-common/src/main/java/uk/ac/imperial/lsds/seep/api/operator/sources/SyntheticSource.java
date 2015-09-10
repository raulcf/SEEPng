package uk.ac.imperial.lsds.seep.api.operator.sources;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalOperator;

public class SyntheticSource implements StaticConnectable, StaticSource {

	private int id;
	private Properties properties;
	
	private SyntheticSource(int id, Properties p) {
		this.id = id;
		this.properties = p;
	}
	
	public static SyntheticSource newSource(int id, Properties p) {
		return new SyntheticSource(id, p);
	}
	
	@Override
	public void connectTo(LogicalOperator operator, Schema schema, int streamId) {
		ConnectionType type = ConnectionType.ONE_AT_A_TIME;
		this.connectTo(operator, schema, streamId, type);
	}

	@Override
	public void connectTo(LogicalOperator operator, Schema schema,
			int streamId, ConnectionType connType) {
		DataStore ds = new DataStore(schema, DataStoreType.FILE, properties);
		((SeepLogicalOperator)operator).reverseConnection(this, streamId, ds, connType);
	}

}
