package uk.ac.imperial.lsds.seep.integration.performance.microbenchmarks;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;

public class ITupleAccessCostTest {

	@Test
	public void testITupleAccessCost() {
		int u = 15;
		long t = System.currentTimeMillis();
		
		Schema outputSchema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		OTuple output = new OTuple(outputSchema);
		
		byte[] serializedData = OTuple.create(outputSchema, new String[]{"userId", "ts"}, new Object[]{u, t});
		
		ITuple input = new ITuple(outputSchema); // share output and input schema in the simplest case
		
		// Test get by name
		int repetitions = 1000000;
		
		long s1 = System.nanoTime();
		for(int i = 0; i<repetitions; i++) {
			input.setData(serializedData);
			
			int userId = input.getInt("userId");
			long ts = input.getLong("ts");
			assertEquals(u, userId);
			assertEquals(t, ts);
		}
		long e1 = System.nanoTime();
		
		long s2 = System.nanoTime();
		for(int i = 0; i<repetitions; i++) {
			input.setData(serializedData);
			
			int idx_userId = input.getIndexFor("userId");
			int idx_ts = input.getIndexFor("ts");
			
			int userId = input.getInt(idx_userId);
			long ts = input.getLong(idx_ts);
			assertEquals(u, userId);
			assertEquals(t, ts);
		}
		long e2 = System.nanoTime();
		
		System.out.println("TIME by name: " + (e1-s1));
		System.out.println("TIME by index: " + (e2-s2));
	}
}
