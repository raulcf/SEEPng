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

public class WorkerWorkerDataCommunicationTest {

	public static void main(String args[]){
		WorkerWorkerDataCommunicationTest wwdct = new WorkerWorkerDataCommunicationTest();
		
		Schema intLong = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		Schema string3 = SchemaBuilder.getInstance().newField(Type.STRING, "sentence")
				.newField(Type.STRING, "word").newField(Type.STRING,  "pos").build();
		
		
		wwdct.execute(intLong);
	}
	
	public void execute(Schema s) {
		WorkerWorkerDataCommunicationTest wwdct = new WorkerWorkerDataCommunicationTest();
		
		// Create inputAdapter map that is used to configure networkselector
		int opId = 99;
		int streamId = 100;
		Map<Integer, InputAdapter> iapMap = null;
		iapMap = new HashMap<>();
		Properties p = new Properties();
		p.setProperty("master.ip", "127.0.0.1");
		p.setProperty("batch.size", "10000");
		p.setProperty("properties.file", "");
		WorkerConfig fake = new WorkerConfig(p);
		NetworkDataStream nds = new NetworkDataStream(new WorkerConfig(p), opId, streamId, s);
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
		int dataPort = listeningPort;
		ds.configureAccept(myIp, listeningPort);
		
		// create outputbuffer for the client
		Connection c = new Connection(new EndPoint(streamId, myIp, listeningPort, dataPort));
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
		
		ds.startNetworkSelector();
		
		/** Continuous sending **/
		int interWriteTime = -1;
		Writer w = wwdct.new Writer(streamId, ob, ds, interWriteTime, s);
		Thread writer = new Thread(w);
		writer.setName("ImTheWriter");
		
		Reader r = wwdct.new Reader(nds);
		Thread reader = new Thread(r);
		reader.setName("ImTheReader");
		
		reader.start();
		writer.start();
		
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
	
	class Writer implements Runnable{

		int clientId;
		OutputBuffer ob;
		NetworkSelector ds;
		int sleep;
		Schema s;
		
		public Writer(int clientId, OutputBuffer ob, NetworkSelector ds, int sleep, Schema s){
			this.clientId = clientId;
			this.ob = ob;
			this.ds = ds;
			this.sleep = sleep;
			this.s = s;
		}
		
		@Override
		public void run() {
			int userId = 0;
			long ts = System.currentTimeMillis();
			while(true){
				ts = System.currentTimeMillis();
				userId++;
				byte[] serializedData = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{userId, ts});
				//byte[] serializedData = OTuple.create(s, new String[]{"sentence", "word", "pos"}, 
				//		new Object[]{"This is an example sentence used to make a point about a buggy system", "system", "UNR"});
				boolean complete = ob.write(serializedData);
				if(complete){
					ds.readyForWrite(ob.id());
				}
				if(sleep > -1){
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}	
		}	
	}
	
	class Reader implements Runnable{
		NetworkDataStream nds;
		public Reader(NetworkDataStream nds){
			this.nds = nds;
		}
		@Override
		public void run() {
			int counter = 0;
			long ts = System.currentTimeMillis();
			while(true){
				DataItem dataItem = nds.pullDataItem(500); // blocking until there's something to receive
				if(dataItem != null){
					boolean consume = true;
					while(consume){
						ITuple incomingTuple = dataItem.consume();
						if(incomingTuple != null){
							counter++;
						}
						else{
							consume = false;
						}
					}
				}
				if((System.currentTimeMillis()) - ts > 1000){
					System.out.println("e/s: "+counter);
					counter = 0;
					ts = System.currentTimeMillis();
				}
			}
		}	
	}
}