package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class MasterControlEndPoint implements SeepEndPoint {

	private final int id;
	private final String ip;
	private final int port;
	
	public MasterControlEndPoint(int id, String ip, int port) {
		this.id = id;
		this.ip = ip;
		this.port = port;
	}
	
	@Override
	public int getId() {
		return id;
	}
	
	@Override
	public int getType() {
		return SeepEndPointType.MASTER_CONTROL.ofType();
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
		return port;
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
		sb.append(this.ip.toString()+" master-worker port: "+this.port);
		return sb.toString();
	}
	
	/**
	 * Empty constructor for kryo serialization
	 */
	public MasterControlEndPoint() {
		this.id = 0;
		this.ip = null;
		this.port = 0;
	}

}
