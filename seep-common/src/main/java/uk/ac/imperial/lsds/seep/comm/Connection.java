package uk.ac.imperial.lsds.seep.comm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.errors.InvalidEndPointException;
import uk.ac.imperial.lsds.seep.errors.SeepException;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPointType;
import uk.ac.imperial.lsds.seep.util.Utils;

public class Connection {

	final private static Logger LOG = LoggerFactory.getLogger(Connection.class);
	
	private final SeepEndPoint ep;
	private Socket socket;
	
	public Connection(SeepEndPoint ep) {
		boolean valid = ep.isValid();
		if(!valid){
			throw new InvalidEndPointException("No IP defined for the endPoint");
		}
		LOG.trace("Created connection Object with EndPoint: {}", ep.toString());
		this.ep = ep;
	}
	
	public int getId() {
		return ep.getId();
	}
	
	public Socket getSocket() {
		return socket;
	}
	
	public Socket getOpenSocket() throws IOException {
		if(socket == null || socket.isClosed()){
			socket = new Socket(ep.getIp(), ep.getPort());
			return socket;
		}
		else if(socket != null){
			if(socket.isConnected()) {
				return socket;
			}
		}
		// TODO: reopen if closed
		
		return null;
	}
	
	public InetSocketAddress getInetSocketAddress(SeepEndPointType type) {
		if(type.ofType() != ep.getType()) {
			// TODO: handle error this properly
			throw new SeepException("Request wrong type of socket..... ##### FIX THIS");
		}
		return new InetSocketAddress(this.ep.getIp(), this.ep.getPort());
	}
	
	public void destroy() {
		try {
			this.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("IP: "+ep.getIp().toString()+" port: "+ep.getPort());
		sb.append(Utils.NL);
		if(socket != null){
			sb.append("ConnectionStatus: "+socket.toString());
		}
		else{
			sb.append("ConnectionStatus: NULL");
		}
		return sb.toString();
	}
	
}
