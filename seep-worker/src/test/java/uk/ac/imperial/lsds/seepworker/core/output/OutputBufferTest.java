package uk.ac.imperial.lsds.seepworker.core.output;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.testutils.MockChannel;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.InputBuffer;

public class OutputBufferTest {

	@Test
	public void test() {
		
		int batchSize = 1024;
		OBuffer ob = createOutputBufferWith(16);
		
		MockChannel channel = new MockChannel(ByteBuffer.allocate(batchSize * 4)); // make sure there's space
		
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "a").newField(Type.INT, "b").newField(Type.INT, "c").build();
		byte[] srcData = OTuple.create(s, new String[]{"a", "b", "c"}, new Object[]{1, 0, 1});
		int tupleSize = srcData.length;
		ITuple iData = new ITuple(s);
		
		Thread writer = new Thread(new Runnable () {
			int tuples = 100;
			public void run() {
				while(tuples > 0) {
					// Write tuple to the output buffer
					ob.write(srcData);
					tuples--;
				}
			}
		});
		
		Thread reader = new Thread(new Runnable () {
			int tuples = 100;
			public void run() {
				while(tuples > 10) {
					
					// Drain tuples to channel
					boolean readAll = ob.drainTo(channel);
					
					if(! readAll) {
						continue;
					}
					// prepare channel to read
					channel.flip();
					
					// Read tuples from channel and check correctness
					ByteBuffer read = ByteBuffer.allocate(tupleSize + TupleInfo.PER_BATCH_OVERHEAD_SIZE + TupleInfo.TUPLE_SIZE_OVERHEAD);
					try {
						channel.read(read);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					read.flip();
					read.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE + TupleInfo.TUPLE_SIZE_OVERHEAD);
					byte[] payload = new byte[tupleSize];
					read.get(payload);
					iData.setData(payload);
					System.out.println(iData.getInt("a") +" "+ iData.getInt("b")+ " "+iData.getInt("c"));
					channel.clear();
					tuples --;
				}
			}
		});
		
		writer.start();
		reader.start();
		
		try {
			reader.join();
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assert(true);
	}

	private OBuffer createOutputBufferWith(int batchSize){
		OutputBuffer ob = new OutputBuffer(null, batchSize);
		return ob;
	}
	
}
