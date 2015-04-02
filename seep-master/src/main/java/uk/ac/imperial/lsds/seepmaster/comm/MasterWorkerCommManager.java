package uk.ac.imperial.lsds.seepmaster.comm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.protocol.BootstrapCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.DeadWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerProtocolAPI;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class MasterWorkerCommManager {

	final private Logger LOG = LoggerFactory.getLogger(MasterWorkerCommManager.class.getName());
	
	private ServerSocket serverSocket;
	private Kryo k;
	private Thread listener;
	private boolean working = false;
	private MasterWorkerAPIImplementation api;
	
	public MasterWorkerCommManager(int port, MasterWorkerAPIImplementation api){
		this.api = api;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		try {
			serverSocket = new ServerSocket(port);
			LOG.info(" Listening on {}:{}", InetAddress.getLocalHost(), port);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		listener = new Thread(new CommMasterWorker());
		listener.setName(CommMasterWorker.class.getSimpleName());
		// TODO: set uncaughtexceptionhandler
	}
	
	public void start(){
		this.working = true;
		LOG.info("Start MasterWorkerCommManager");
		this.listener.start();
	}
	
	public void stop(){
		//TODO: do some other cleaning work here
		this.working = false;
	}
	
	class CommMasterWorker implements Runnable{

		@Override
		public void run() {
			while(working){
				BufferedReader bis = null;
				Input i = null;
				Socket incomingSocket = null;
				try{
					// Blocking call
					incomingSocket = serverSocket.accept();
					
					InputStream is = incomingSocket.getInputStream();
					i = new Input(is);
					
					MasterWorkerCommand command = k.readObject(i, MasterWorkerCommand.class);
					short type = command.type();
					
					if(type == MasterWorkerProtocolAPI.BOOTSTRAP.type()){
						LOG.info("RX-> Bootstrap command");
						BootstrapCommand bc = command.getBootstrapCommand();
						api.bootstrapCommand(bc);
					}
					else if(type == MasterWorkerProtocolAPI.CRASH.type()){
						LOG.info("RX-> Crash command");
					}
					else if(type == MasterWorkerProtocolAPI.DEADWORKER.type()){
						LOG.info("RX-> DeadWorker command");
						DeadWorkerCommand dwc = command.getDeadWorkerCommand();
						api.handleDeadWorker(dwc);
					}
				}
				catch(IOException io){
					io.printStackTrace();
				}
				finally {
					if (incomingSocket != null){
						try {
							i.close();
							incomingSocket.close();
						}
						catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}	
		}
	}
}
