package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.data.Schema;

public interface Connectable {
	// methods to connect operators
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema);
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType);
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, DataStore dSrc);
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType, DataStore dSrc);
}
