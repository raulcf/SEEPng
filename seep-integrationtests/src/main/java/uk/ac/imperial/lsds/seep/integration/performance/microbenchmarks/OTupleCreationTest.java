package uk.ac.imperial.lsds.seep.integration.performance.microbenchmarks;

import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;

public class OTupleCreationTest {

	public static void main(String args[]){
		
		OTupleCreationTest otct = new OTupleCreationTest(); 
		otct.allIn(); // creating OTuple on demand always
		otct.allOut(); // creating as much out as possible
		
	}
	
	public void allIn(){
		long time = System.currentTimeMillis();
		int count = 0;
		int period = 1000; //1 s
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		int userId = 0;
		long ts = 0;
		while(true){
			count++;
			userId++;
			ts++;
			byte[] ser = OTuple.create(schema, new String[]{"userId",  "ts"}, new Object[]{userId, ts});
			if((System.currentTimeMillis() - time) > period){
				System.out.println("ser/s: "+count);
				count = 0;
				time = System.currentTimeMillis();
				ser = null;
			}
		}
	}
	
	public void allOut(){
		long time = System.currentTimeMillis();
		int count = 0;
		int period = 1000; //1 s
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		int userId = 0;
		long ts = 0;
		String[] fields = new String[]{"userId", "ts"};
		Object[] values = new Object[]{userId, ts};
		while(true){
			count++;
			userId++;
			ts++;
			values[0] = userId;
			values[1] = ts;
			byte[] ser = OTuple.create(schema, fields, values);
			if((System.currentTimeMillis() - time) > period){
				System.out.println("ser/s: "+count);
				count = 0;
				time = System.currentTimeMillis();
				ser = null;
			}
		}
	}
}
