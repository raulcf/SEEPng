package uk.ac.imperial.lsds.seep.infrastructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlEndPoint implements SeepEndPoint {

	final private static Logger LOG = LoggerFactory.getLogger(ControlEndPoint.class);
	
	final private int id;
	final private String ip;
	final private int port;
	
	public ControlEndPoint(int id, String ip, int port) {
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
		return SeepEndPointType.CONTROL.ofType();
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

	@Override
	public boolean isValid() {
		if(ip == null) return false;
		return true;
	}
	
	@Override
	public int hashCode() {
		return id;
	}
	
	@Override
	public boolean equals(Object cep) {
		return cep.hashCode() == this.hashCode();
	}
	
	/**
	 * Empty constructor for kryo serialization
	 */
	public ControlEndPoint() {
		this.id = 0;
		this.ip = null;
		this.port = 0;
	}

}
