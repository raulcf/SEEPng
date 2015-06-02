package uk.ac.imperial.lsds.seep.api.operator.sources;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalOperator;

public class FileSource implements StaticConnectable, StaticSource {

	private int id;
	private Properties properties;
	
	private FileSource(int id, Properties p) {
		this.id = id;
		this.properties = p;
	}
	
	public static FileSource newSource(int id, Properties p) {
		return new FileSource(id, p);
	}

	@Override
	public void connectTo(LogicalOperator operator, Schema schema, int streamId, ConnectionType connType) {
		// I'm a virtual op, so no need to say what is my downstream. Only need to indicate I'm its upstream
		DataStore ds = new DataStore(schema, DataStoreType.FILE, properties);
		((SeepLogicalOperator)operator).reverseConnection(this, streamId, ds, connType);
	}

	@Override
	public void connectTo(LogicalOperator operator, Schema schema, int streamId) {
		ConnectionType type = ConnectionType.ONE_AT_A_TIME;
		this.connectTo(operator, schema, streamId, type);
	}

}
