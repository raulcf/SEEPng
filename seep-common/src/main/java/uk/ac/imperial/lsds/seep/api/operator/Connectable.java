package uk.ac.imperial.lsds.seep.api.operator;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;

public interface Connectable {
	// methods to connect operators
	public void connectTo(Operator downstreamOperator, int streamId, DataStore dataStore);
	public void connectTo(Operator downstreamOperator, int streamId, DataStore dataStore, ConnectionType connectionType);
}
