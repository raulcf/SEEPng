package uk.ac.imperial.lsds.seep.api.operator.sources;

import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalOperator;

public class TextFileSource implements StaticConnectable, TaggingSource {

	private int id;
	private Properties properties;
	
	private TextFileSource(int id, Properties p) {
		this.id = id;
		this.properties = p;
		this.properties.setProperty(FileConfig.TEXT_SOURCE, new Boolean(true).toString());
	}
	
	public static TextFileSource newSource(int id, Properties p) {
		return new TextFileSource(id, p);
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
