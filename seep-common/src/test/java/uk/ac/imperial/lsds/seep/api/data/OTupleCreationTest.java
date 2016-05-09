package uk.ac.imperial.lsds.seep.api.data;

import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class OTupleCreationTest {

int period = 1000; // 1 second
	
	public static void main(String args[]){
		
		OTupleCreationTest otct = new OTupleCreationTest(); 
//		otct.allIn(); // creating OTuple on demand always
//		otct.allOut(); // creating as much out as possible
//		otct.allOutUnsafe(); // unsafe creation
		
		int num = 100000000; // 100M
		
		long s1 = System.nanoTime();
		otct.createSafeOTuple(num);
		long e1 = System.nanoTime();
		
		long s2 = System.nanoTime();
		otct.createUnsafeOTuple(num);
		long e2 = System.nanoTime();
		
		System.out.println("Time to create "+num+" tuples with safe:  " + (e1-s1));
		System.out.println("Time to create "+num+" tuples with UNsafe:  " + (e2-s2));
		
	}
	
	public void createSafeOTuple(int num) {
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		int userId = 0;
		long ts = 0;
		String[] fields = new String[]{"userId", "ts"};
		Object[] values = new Object[]{userId, ts};
		while(num > 0) {
			num--;
			byte[] ser = OTuple.create(schema, fields, values);
		}
 	}
	
	public void createUnsafeOTuple(int num) {
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		int userId = 0;
		long ts = 0;
		Object[] values = new Object[]{userId, ts};
		Type[] types = new Type[]{Type.INT, Type.LONG};
		while(num > 0) {
			num--;
			byte[] ser = OTuple.createUnsafe(types, values, 12);
		}
	}
	
	public void allIn(){
		System.out.println("ALL IN");
		long time = System.currentTimeMillis();
		int count = 0;
		
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
		System.out.println("ALL OUT");
		long time = System.currentTimeMillis();
		int count = 0;
		
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
	
	public void allOutUnsafe(){
		System.out.println("ALL OUT UNSAFE");
		long time = System.currentTimeMillis();
		int count = 0;
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		int userId = 0;
		long ts = 0;
		Type[] types = new Type[]{Type.INT, Type.LONG};
		String[] fields = new String[]{"userId", "ts"};
		Object[] values = new Object[]{userId, ts};
		while(true){
			count++;
			userId++;
			ts++;
			values[0] = userId;
			values[1] = ts;
			byte[] ser = OTuple.createUnsafe(types, values, 12);
			if((System.currentTimeMillis() - time) > period){
				System.out.println("ser/s: "+count);
				count = 0;
				time = System.currentTimeMillis();
				ser = null;
			}
		}
	}
}
