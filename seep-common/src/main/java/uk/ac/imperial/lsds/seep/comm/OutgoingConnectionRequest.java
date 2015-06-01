package uk.ac.imperial.lsds.seep.comm;

import uk.ac.imperial.lsds.seep.core.OBuffer;

public class OutgoingConnectionRequest {
	
	public OutgoingConnectionRequest(Connection connection, OBuffer oBuffer) {
		this.connection = connection;
		this.oBuffer = oBuffer;
	}
	
	public Connection connection;
	public OBuffer oBuffer;
	
}
