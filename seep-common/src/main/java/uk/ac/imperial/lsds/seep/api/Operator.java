package uk.ac.imperial.lsds.seep.api;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.state.SeepState;

public interface Operator extends Connectable {

	// id, name and type (stateful or stateless)
	public int getOperatorId();
	public String getOperatorName();
	public boolean isStateful();
	public SeepState getState();
	// task
	public SeepTask getSeepTask();
	// connections to other logical operators
	public List<DownstreamConnection> downstreamConnections();
	public List<UpstreamConnection> upstreamConnections();
	
	public String toString();
	
}
