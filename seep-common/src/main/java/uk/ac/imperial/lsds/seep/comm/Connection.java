package uk.ac.imperial.lsds.seep.comm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;

public class Connection {

	final private static Logger LOG = LoggerFactory.getLogger(Connection.class);
	
	private final EndPoint ep;
	private Socket s;
	
	public Connection(EndPoint ep) {
		boolean valid = ep.isValid();
		if(!valid){
			throw new InvalidEndPointException("No IP defined for the endPoint");
		}
		LOG.trace("Created connection with EndPoint: {}", ep.toString());
		this.ep = ep;
	}
	
	public int getId(){
		return ep.getId();
	}
	
	public Socket getSocket(){
		return s;
	}
	
	public Socket getOpenSocket() throws IOException{
		if(s == null || s.isClosed()){
			s = new Socket(ep.getIp(), ep.getPort());
			return s;
		}
		else if(s != null){
			if(s.isConnected()) {
				return s;
			}
		}
		// TODO: reopen if closed
		
		return null;
	}
	
	public InetSocketAddress getInetSocketAddress(){
		return new InetSocketAddress(this.ep.getIp(), this.ep.getPort());
	}
	
	public InetSocketAddress getInetSocketAddressForData(){
		LOG.trace("Building InetSocketAddress with IP: {}, dataPort: {}", this.ep.getIp(), this.ep.getDataPort());
		return new InetSocketAddress(this.ep.getIp(), this.ep.getDataPort());
	}
	
	public void destroy(){
		try {
			this.s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("IP: "+ep.getIp().toString()+" port: "+ep.getPort());
		sb.append(Utils.NL);
		if(s != null){
			sb.append("ConnectionStatus: "+s.toString());
		}
		else{
			sb.append("ConnectionStatus: NULL");
		}
		return sb.toString();
	}
	
}
