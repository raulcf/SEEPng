package uk.ac.imperial.lsds.seepworker.core;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.DatasetInputAdapter;

public class WMemSDskWDskLMemRMemTest {

	Schema s = SchemaBuilder.getInstance().newField(Type.INT, "v1").newField(Type.INT, "v2").build();

	private WorkerConfig buildWorkerConfig() {
		
		// configure mem execution
		final int tupleSize = 8 + TupleInfo.TUPLE_SIZE_OVERHEAD;
		final int numTuples = 10;
		//Basing the buffer sizes on the tuple size lets us control running out of memory
		final int maxBufferSize = numTuples * tupleSize;
		final int minBufferAllocation = 2 * tupleSize;
		
		
		Properties p = new Properties();
		p.setProperty(WorkerConfig.MASTER_IP, "");
		p.setProperty(WorkerConfig.PROPERTIES_FILE, "");
		//p.setProperty(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH, "100");
		p.setProperty(WorkerConfig.WORKER_IP, "");
		
		// mem properties
		p.put("bufferpool.max.memory.available", maxBufferSize);
		p.put("bufferpool.min.buffer.size", minBufferAllocation);
		
		return new WorkerConfig(p);
	}
	
	@Test
	public void testWMemSDskLMemRMem() {
		
		WorkerConfig wc = buildWorkerConfig();
		DataReferenceManager drm = null;
		drm = DataReferenceManager.makeDataReferenceManager(wc);
		DataStore dataStore = new DataStore(s, DataStoreType.IN_MEMORY);
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(0, dataStore, null, ServeMode.STORE);
		OBuffer testDataset = drm.manageNewDataReference(dataRef);
		InputAdapter ia = new DatasetInputAdapter(wc, 0, (Dataset)testDataset);
		
		// Write 10 tuples to memory
		int written = 0;
		for (int i = 0; i < 10; i++) {
			
			byte[] srcData = OTuple.create(s, new String[]{"v1", "v2"}, new Object[]{i, i+1});
			
			testDataset.write(srcData, null);
			written++;
		}
		
		// Spill to disk
		try {
			drm.sendDatasetToDisk(testDataset.getDataReference().getId());
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Write 10 tuples to disk
		for (int i = 10; i < 20; i++) {
			
			byte[] srcData = OTuple.create(s, new String[]{"v1", "v2"}, new Object[]{i, i+1});
			
			testDataset.write(srcData, null);
			written++;
		}
		
		// Load to memory
		drm.retrieveDatasetFromDisk(testDataset.getDataReference().getId());
		
		// Written
		int v1 = 0, v2 = 0;
		boolean run = true;
		int read = 0;
		while(run) {
			ITuple iData = ia.pullDataItem(500);
			if(iData == null) break;
			v1 = iData.getInt("v1");
			v2 = iData.getInt("v2");
			System.out.println("v1: " +v1+ " v2: " +v2);
			read++;
		}
		System.out.println("W: " +written+ " R: " +read);
		assert(written == read);
	}
}
