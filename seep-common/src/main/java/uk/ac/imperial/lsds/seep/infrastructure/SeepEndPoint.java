package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;

public interface SeepEndPoint {
	
	public int getId();
	public int getType();
	public InetAddress getIp();
	public int getPort();
	public boolean isValid();
	
}
