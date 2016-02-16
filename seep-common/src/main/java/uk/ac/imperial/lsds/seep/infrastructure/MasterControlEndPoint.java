package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MasterControlEndPoint implements SeepEndPoint {

	final private static Logger LOG = LoggerFactory.getLogger(MasterControlEndPoint.class);
	
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
			LOG.error("The IP: {} is unrecognized", ip);
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
	public MasterControlEndPoint() {
		this.id = 0;
		this.ip = null;
		this.port = 0;
	}

}
