package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class EndPoint {

	private final int id;
	private final String ip;
	private final int port;
	private final int dataPort;
	
	public EndPoint(int id, InetAddress ip, int port){
		this.id = id;
		this.ip = ip.getHostName();
		this.port = port;
		this.dataPort = -1; // no data connection to this endpoint
	}
	
	public EndPoint(int id, InetAddress ip, int port, int dataPort){
		this.id = id;
		this.ip = ip.getHostName();
		this.port = port;
		this.dataPort = dataPort;
	}
	
	public int getId(){
		return id;
	}

	public InetAddress getIp() {
		try {
			return InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int getPort() {
		return port;
	}
	
	public int getDataPort(){
		return dataPort;
	}
	
	/**
	 * An EndPoint is valid is its IP is not null
	 * @return
	 */
	public boolean isValid(){
		if(ip == null) return false;
		return true;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(this.ip.toString()+" port: "+this.port+" d_port: "+this.dataPort);
		return sb.toString();
	}
}
