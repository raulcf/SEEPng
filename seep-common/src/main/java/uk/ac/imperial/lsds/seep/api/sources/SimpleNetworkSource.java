package uk.ac.imperial.lsds.seep.api.sources;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;

public class SimpleNetworkSource implements SeepTask {

	private Thread connHandler;
	private int localPort;
	private boolean working = true;
	
	public SimpleNetworkSource(int localPort){
		this.localPort = localPort;
	}
	
	@Override
	public void setUp() {
		connHandler = new Thread(new ConnectionHandler());
		connHandler.start();
	}

	@Override
	public void processData(ITuple data, API api) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		this.working = false;
		while(connHandler.isAlive()); // make sure the thread exits properly to avoid problems here
		
	}
	
	class ConnectionHandler implements Runnable{

		@Override
		public void run() {
			try {
				ServerSocket ss = new ServerSocket(localPort);
				Socket incomingConnection = ss.accept();
				InputStream is = incomingConnection.getInputStream();
				while(working){
					// Read data from network with the right schema
				}
				is.close();
				ss.close();
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
