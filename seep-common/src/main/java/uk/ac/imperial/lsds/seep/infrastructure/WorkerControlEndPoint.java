package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class WorkerControlEndPoint implements SeepEndPoint {

	private final int id;
	private final String ip;
	private final int controlPort;
	
	public WorkerControlEndPoint(int id, String ip, int controlPort) {
		this.id = id;
		this.ip = ip;
		this.controlPort = controlPort;
	}
	
	@Override
	public int getId() {
		return id;
	}
	
	@Override
	public int getType() {
		return SeepEndPointType.WORKER_CONTROL.ofType();
	}

	@Override
	public InetAddress getIp() {
		try {
			return InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public int getPort() {
		return controlPort;
	}

	/**
	 * An EndPoint is valid is its IP is not null
	 * @return
	 */
	@Override
	public boolean isValid() {
		if(ip == null) return false;
		return true;
	}

	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(this.ip.toString()+" controlPort: "+this.controlPort);
		return sb.toString();
	}
	
	@Override
	public int hashCode() {
		return id;
	}
	
	@Override
	public boolean equals(Object obj){
		if(this.hashCode() == obj.hashCode()) return true;
		return false;
	}
	
	/**
	 * Empty constructor for kryo serialization
	 */
	public WorkerControlEndPoint() {
		this.id = 0;
		this.ip = null;
		this.controlPort = 0;
	}

}
