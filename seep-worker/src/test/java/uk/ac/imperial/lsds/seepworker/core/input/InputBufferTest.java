package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seep.testutils.MockChannel;

public class InputBufferTest {

	@Test
	public void test() {
		String queueLength = "100";
		int tuples = Integer.parseInt(queueLength);
		InputBuffer ib = createInputBufferWith(queueLength, tuples);
		
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "a").newField(Type.INT, "b").newField(Type.INT, "c").build();
		ITuple iData = new ITuple(s);
		
		for(int i = 0; i < tuples; i++) {
			byte[] data = OTuple.create(s, new String[]{"a", "b", "c"}, new Object[]{i, i+1, i+2});
			ib.pushData(data);
		}
		
		for(int i = 0; i < tuples; i++) {
			byte[] data = ib.read(0);
			iData.setData(data);
			System.out.println(iData.getInt("a") +" "+ iData.getInt("b")+ " "+iData.getInt("c"));
		}
		
		assert(true);
	}
	
	@Test
	public void testThroughChannel() {
		String queueLength = "100";
		int tuples = Integer.parseInt(queueLength);
		InputBuffer ib = createInputBufferWith(queueLength, tuples);
		
		Schema s = SchemaBuilder.getInstance().newField(Type.INT, "a").newField(Type.INT, "b").newField(Type.INT, "c").build();
		ITuple iData = new ITuple(s);
		
		byte[] srcData = OTuple.getWireBytes(s, new String[]{"a", "b", "c"}, new Object[]{1, 0, 1});
		
		// Write to channel
		ByteBuffer b = ByteBuffer.allocate(tuples * srcData.length);
		MockChannel channel = new MockChannel(b);
		System.out.println("Channel with capacity: " + channel.capacity());
		
		for(int i = 0; i < tuples; i++) {
			ByteBuffer backup = ByteBuffer.wrap(srcData);
			try {
				channel.write(backup);
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Prepare channel for read
		channel.flip();
		
		// Read from channel to internal queue
		while(channel.hasRemaining()) {
			ib.readFrom(channel);
		}
		
		// Read from internal queue
		for(int i = 0; i < tuples; i++) {
			byte[] data = ib.read(0);
			if(data == null) {
				System.out.println("data is null");
			}
			iData.setData(data);
			System.out.println(iData.getInt("a") +" "+ iData.getInt("b")+ " "+iData.getInt("c"));
		}
		
		assert(true);
	}
	
	private InputBuffer createInputBufferWith(String queueLength, int tuples){
		Properties p = new Properties();
		p.setProperty(WorkerConfig.MASTER_IP, "");
		p.setProperty(WorkerConfig.PROPERTIES_FILE, "");
		p.setProperty(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH, queueLength);
		WorkerConfig wc = new WorkerConfig(p);
		InputBuffer ib = InputBuffer.makeInputBufferFor(wc, null);
		return ib;
	}

}
