package uk.ac.imperial.lsds.seep.api.operator.sources;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;

public interface StaticConnectable {

	public void connectTo(LogicalOperator operator, Schema schema, int streamId);
	public void connectTo(LogicalOperator operator, Schema schema, int streamId, ConnectionType connType);
	
}
