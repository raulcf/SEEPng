package uk.ac.imperial.lsds.seepworker.core;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.testutils.WriterOfTrash;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutputFactory;

public class OutOfMemoryEventTest {

	public SeepTask createFakeSeepTask(Schema schema) {
		WriterOfTrash wot = new WriterOfTrash(schema);
		return wot;
	}
	
	@Test
	public void testRunOutOfMemoryAndSpillToDisk() {
		
		// Configure test execution
		final int numBytesWritten = 1048576;
		final int tupleSize = 4 + TupleInfo.TUPLE_SIZE_OVERHEAD;
		final int numTuples = numBytesWritten / tupleSize;
		final int maxBufferSize = 2046;
		final int minBufferAllocation = 128;
		// false if im writing less, true if Im writing more than memory size
		final boolean eventPositive = ((numTuples*tupleSize) > maxBufferSize); 
		
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "4bytes").build();
		// Get the fake task, passing a schema
		SeepTask wot = createFakeSeepTask(schema);
		
		// Configure properties of data reference manager
		Properties p = new Properties();
		p.put("bufferpool.max.memory.available", maxBufferSize);
		p.put("bufferpool.min.buffer.size", minBufferAllocation);
		p.put("master.ip", ""); // mandatory config
		p.put("worker.ip", ""); // mandatory config
		p.put("properties.file", ""); // mandatory config
		WorkerConfig wc = new WorkerConfig(p);
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(wc);
		CoreOutput coreOutput = CoreOutputFactory.buildCoreOutputForTestingOneDatasetOutput(null, drm);
		API api = new Collector(0, coreOutput);
		
		ITuple d = new ITuple(schema); // same input schema than output, who cares
		
		for(int i = 0; i < numTuples; i++) {
			// create data
			byte[] data = new byte[] {0,1,2,3};
			d.setData(data);

			// call the task
			wot.processData(d, api);
		}
		
		List<RuntimeEvent> evs = api.getRuntimeEvents();
		
		for(RuntimeEvent ev : evs) {
			if (eventPositive){
				assert(ev.getSpillToDiskRuntimeEvent() != null);
			}
			else if (! eventPositive) {
				assert(ev.getSpillToDiskRuntimeEvent() == null);
			}
		}
	}
	
	@Test
	public void testReadFromSpilledFile() {
		// First write something to the dataset -> REUSE method above, maybe changing the payload to ease debugging
		
		// Then read data back and make sure size is the same and payload is correct

		// Configure test execution
		final int tupleSize = 4 + TupleInfo.TUPLE_SIZE_OVERHEAD;
		final int numTuples = 6;
		//Basing the buffer sizes on the tuple size lets us control running out of memory
		final int maxBufferSize = 5 * tupleSize;
		final int minBufferAllocation = 2 * tupleSize;
				
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "4bytes").build();
		// Get the fake task, passing a schema
		SeepTask wot = createFakeSeepTask(schema);
		
		// Configure properties of data reference manager
		Properties p = new Properties();
		p.put("bufferpool.max.memory.available", maxBufferSize);
		p.put("bufferpool.min.buffer.size", minBufferAllocation);
		p.put("master.ip", ""); // mandatory config
		p.put("worker.ip", ""); // mandatory config
		p.put("properties.file", ""); // mandatory config
		WorkerConfig wc = new WorkerConfig(p);
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(wc);
		CoreOutput coreOutput = CoreOutputFactory.buildCoreOutputForTestingOneDatasetOutput(null, drm);
		API api = new Collector(0, coreOutput);
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(((int)System.currentTimeMillis()%Integer.MAX_VALUE), null, null, ServeMode.STORE);
		Dataset testDataset = (Dataset) drm.manageNewDataReference(dataRef);
		
		ITuple d = new ITuple(schema); // same input schema than output, who cares
		
		//create an output Dataset which is larger than our buffer
		for(int i = 0; i < numTuples; i++) {
			// create data
			ByteBuffer writeData = ByteBuffer.allocate(tupleSize);
			writeData.putInt((new Integer(Integer.BYTES)));
			writeData.putInt(i);
			byte[] data = writeData.array();
			d.setData(data);
			testDataset.write(writeData.array(), api);

			// call the task
			//wot.processData(d, api);
		}
		List<RuntimeEvent> evs = api.getRuntimeEvents();
		Boolean checkForSpill = new Boolean(false);
		for(RuntimeEvent ev : evs) {
			checkForSpill = checkForSpill || (ev.getSpillToDiskRuntimeEvent() != null);
		}
		assert(checkForSpill):"Did not spill Dataset to disk";
		
		//TODO: Grab output Dataset and check it for numbers 1-25.

		for (int x = 0; x < numTuples; x++) {
	        byte[] content = testDataset.consumeData();
	        assert(content != null):"Problem reading from disk";
			ByteBuffer readData = ByteBuffer.allocate(content.length);
			readData.put(content);
			readData.flip();
			readData.getInt();
			assert(readData.getInt() == x):"The order of the Dataset appears to have been altered by the cache/return process";
		}
			
		//TODO: Assert that the Dataset has been fully consumed.
		assert(testDataset.consumeData() == null):"New data was generated during the cache/return process";
	}
	
}
