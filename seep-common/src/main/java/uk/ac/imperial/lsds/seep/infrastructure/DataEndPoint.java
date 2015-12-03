package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class DataEndPoint implements SeepEndPoint {

	private final int id;
	private final String ip;
	private final int dataPort;
	
	public DataEndPoint(int id, String ip, int dataPort) {
		this.id = id;
		this.ip = ip;
		this.dataPort = dataPort;
	}
	
	@Override
	public int getId() {
		return id;
	}
	
	@Override
	public int getType() {
		return SeepEndPointType.DATA.ofType();
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
		return dataPort;
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
		sb.append(this.ip.toString()+" dataPort: "+this.dataPort);
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
	public DataEndPoint() {
		this.id = 0;
		this.ip = null;
		this.dataPort = 0;
	}
}
