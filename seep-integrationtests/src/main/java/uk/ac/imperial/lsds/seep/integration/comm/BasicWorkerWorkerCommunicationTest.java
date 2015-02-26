package uk.ac.imperial.lsds.seep.integration.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.data.DataItem;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.comm.NetworkSelector;
import uk.ac.imperial.lsds.seepworker.core.input.NetworkDataStream;


public class BasicWorkerWorkerCommunicationTest {


	public static void main(String args[]) {
		// Create inputAdapter map that is used to configure networkselector
		int opId = 99;
		int clientId = 100;
		int streamId = 101;
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		Map<Integer, InputAdapter> iapMap = null;
		iapMap = new HashMap<>();
		Properties p = new Properties();
		p.setProperty("master.ip", "127.0.0.1");
		p.setProperty("batch.size", "10");
		p.setProperty("properties.file", "");
		WorkerConfig fake = new WorkerConfig(p);
		NetworkDataStream nds = new NetworkDataStream(new WorkerConfig(p), opId, clientId, s);
		iapMap.put(opId, nds);
		// TODO: build this
		NetworkSelector ds = NetworkSelector.makeNetworkSelectorWithMap(opId, iapMap);
		// Create client and server that will be interchanging data
		InetAddress myIp = null;
		try {
			myIp = InetAddress.getByName("127.0.0.1");
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		int listeningPort = 5555;
		ds.configureAccept(myIp, listeningPort);
		
		// create outputbuffer for the client
		Connection c = new Connection(new EndPoint(clientId, myIp, listeningPort));
		int batch_size = fake.getInt(WorkerConfig.BATCH_SIZE);
		OutputBuffer ob = new OutputBuffer(opId, c, streamId, batch_size);
		Set<OutputBuffer> obs = new HashSet<>();
		obs.add(ob);
		ds.configureConnect(obs);
		
		ds.initNetworkSelector();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		/** 1 send **/
		
		// Create tuple and send it to the other worker
		byte[] serializedData = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{3, 23423L});
		//byte[] serializedData2 = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{4, 8384L});
		System.out.println("Tuple length: "+serializedData.length);
		//ob.write(serializedData);
		ob.write(serializedData);
//		if(canSend){
//			System.out.println("Notifying to send");
//			((EventAPI)ds).readyForWrite(clientId);
//		} else{
//			System.out.println("CANNOT send yet");
//		}
		
		DataItem _incomingTuple = nds.pullDataItem(500); // blocking until there's something to receive
		ITuple incomingTuple = _incomingTuple.consume();
		System.out.println(incomingTuple.toString());
		
//		ITuple incomingTuple2 = nds.pullDataItem(); // blocking until there's something to receive
//		System.out.println(incomingTuple2.toString());
		
		/** 2 send **/
		// Create tuple and send it to the other worker
		byte[] serializedData2 = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{4, 848448L});
		System.out.println("Tuple length: "+serializedData2.length);
		ob.write(serializedData2);
//		if(canSend2){
//			System.out.println("Notifying to send");
//			((EventAPI)ds).readyForWrite(clientId);
//		} else{
//			System.out.println("CANNOT send yet");
//		}
		
		DataItem _incomingTuple2 = nds.pullDataItem(500); // blocking until there's something to receive
		ITuple incomingTuple2 = _incomingTuple2.consume();
		System.out.println(incomingTuple2.toString());
		
		DataItem _incomingTuple3 = nds.pullDataItem(500); // blocking until there's something to receive
		ITuple incomingTuple3 = _incomingTuple3.consume();
		System.out.println(incomingTuple3.toString());
		
		while(true){
			try {
				Thread.sleep(1000);
			} 
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
