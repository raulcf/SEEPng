package uk.ac.imperial.lsds.seepworker.core;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.api.ScheduleBuilder;
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
	}
	
}
