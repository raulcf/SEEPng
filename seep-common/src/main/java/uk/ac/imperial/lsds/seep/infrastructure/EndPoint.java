package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class EndPoint {
	
	private final int id;
	private final String ip;
	private final int port;
	private final int dataPort;
	private final int controlPort;
	
	public EndPoint(int id, InetAddress ip, int port){
		this.id = id;
		this.ip = ip.getHostName();
		this.port = port;
		this.dataPort = -1; // no data connection to this endpoint
		this.controlPort = -1; // no control conenction to this endpoint
	}
	
	public EndPoint(int id, InetAddress ip, int port, int dataPort){
		this.id = id;
		this.ip = ip.getHostName();
		this.port = port;
		this.dataPort = dataPort;
		this.controlPort = -1; // no control connection to this endpoint
	}
	
	public EndPoint(int id, InetAddress ip, int port, int dataPort, int controlPort){
		this.id = id;
		this.ip = ip.getHostName();
		this.port = port;
		this.dataPort = dataPort;
		this.controlPort = controlPort;
	}
	
	public SeepEndPoint extractMasterControlEndPoint() {
		return new MasterControlEndPoint(id, ip, port);
	}
	
	public SeepEndPoint extractWorkerControlEndPoint() {
		return new WorkerControlEndPoint(id, ip, controlPort);
	}
	
	public SeepEndPoint extractDataEndPoint() {
		return new DataEndPoint(id, ip, dataPort);
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
	
	public int getDataPort() {
		return dataPort;
	}
	
	public int getControlPort() {
		return controlPort;
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
	
	/**
	 * Empty constructor for kryo serialization
	 */
	public EndPoint() {
		this.id = 0;
		this.ip = null;
		this.port = 0;
		this.dataPort = 0;
		this.controlPort = 0;
	}
}
