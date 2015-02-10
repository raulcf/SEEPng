package uk.ac.imperial.lsds.seepworker.comm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public class BasicAsyncConTest {

	@Test
	public void testOpenCon(){
		try {
			Connection c = new Connection(new EndPoint(0, InetAddress.getLocalHost(), 2500, 5200));
			SocketChannel channel = SocketChannel.open();
			InetSocketAddress address = c.getInetSocketAddressForData();
		    Socket socket = channel.socket();
		    
			socket.setKeepAlive(true);
			// Unlikely in non-production scenarios we'll be up for more than 2 hours but...
		    socket.setTcpNoDelay(true); // Disabling Nagle's algorithm
		    try {
		    	channel.configureBlocking(false);
		        channel.connect(address);
		    } 
		    catch (UnresolvedAddressException uae) {
		        channel.close();
		        uae.printStackTrace();
		    }
		    catch (IOException io) {
		        channel.close();
		        io.printStackTrace();
		    }
			channel.configureBlocking(false);
		} 
		catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
}
