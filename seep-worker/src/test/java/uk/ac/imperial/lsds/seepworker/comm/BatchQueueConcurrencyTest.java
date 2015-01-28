package uk.ac.imperial.lsds.seepworker.comm;


public class BatchQueueConcurrencyTest {

//	@Test
//	public void test() {
//		Properties p = new Properties();
//		p.setProperty("master.ip", "127.0.0.1");
//		p.setProperty("batch.size", "10");
//		WorkerConfig fake = new WorkerConfig(p);
//		int clientId = 100;
//		InetAddress myIp = null;
//		try {
//			myIp = InetAddress.getByName("127.0.0.1");
//		} 
//		catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//		int listeningPort = 5555;
//		Connection c = new Connection(new EndPoint(clientId, myIp, listeningPort));
//		OutputBuffer ob = new OutputBuffer(fake, clientId, c);
//		
//		Thread writer = new Thread(new Writer(ob));
//		Thread reader = new Thread(new Reader(ob));
//		writer.start();
//		reader.start();
//		
//		while(true){
//			try {
//				Thread.sleep(1000);
//			} 
//			catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}
//	
//	class Writer implements Runnable{
//		
//		OutputBuffer ob;
//		
//		public Writer(OutputBuffer ob){
//			this.ob = ob;
//		}
//		
//		@Override
//		public void run(){
//			Schema s = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
//			int userId = 0;
//			long ts = 0;
//			while(true){
//				userId = userId + 1;
//				ts = ts+1;
//				byte[] data = OTuple.create(s, new String[]{"userId", "ts"}, new Object[]{userId, ts});
//				ob.bq.add(data);
//			}
//		}
//	}
//	
//	class Reader implements Runnable{
//		OutputBuffer ob;
//		
//		public Reader(OutputBuffer ob){
//			this.ob = ob;
//		}
//		@Override
//		public void run(){
//			while(true){
//				List<byte[]> datas = ob.bq.poll();
//				System.out.println("Size of datas: "+datas.size());
//				for(int i = 0; i<datas.size(); i++){
//					byte[] data = datas.get(i);
//					ByteBuffer bb = ByteBuffer.wrap(data);
//					bb.position(0);
//					int userId = bb.getInt();
//					System.out.println("UID: "+userId);
//				}
//				try {
//					Thread.sleep(1);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}
//	}

}
