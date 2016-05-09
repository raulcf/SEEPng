package uk.ac.imperial.lsds.seepworker.core;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;
import uk.ac.imperial.lsds.seepworker.core.Dataset;
import uk.ac.imperial.lsds.seepworker.core.input.DatasetInputAdapter;

public class DiskCacherTest {
	
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

	/***
	 * Simple test that creates a Dataset attached to a DataReferenceManager, 
	 * then checks that said DataReferenceManager successfully tracks when the
	 * Dataset resides in memory and when it has been pushed to disk.
	 */
	@Test
	public void testInMemCall() {
		System.out.println("Testing inMem");
		//make new Dataset
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(buildWorkerConfig());
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(((int)System.currentTimeMillis()%Integer.MAX_VALUE), null, null, ServeMode.STORE);
		Dataset testDataset = (Dataset) drm.manageNewDataReference(dataRef);
		//new Datasets should reside in memory.
		assert(drm.datasetIsInMem(testDataset.id()) == true): "Dataset did not start in memory";
		//cache the Dataset and check that the in memory tracker is updated
		try {
			drm.sendDatasetToDisk(testDataset.id());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(drm.datasetIsInMem(testDataset.id()) == false):"Dataset was sent to disk, but inMem did not update";
		//return the Dataset to memory and check that the in memory tracker is updated again
		drm.retrieveDatasetFromDisk(testDataset.id());
		assert(drm.datasetIsInMem(testDataset.id()) == true):"Dataset returned to memory, but inMem did not update";
	}
	
	/**
	 * Set of tests:
	 * W - write
	 * R - read
	 * Mem - memory
	 * Dsk - disk
	 */
	
	// Write to memory and read from memory
	@Test
	public void testWMemRMem() {
		WorkerConfig wc = buildWorkerConfig();
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(wc);
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
		
		// Written
		int v1 = 0, v2 = 0;
		int read = 0;
		boolean run = true;
		while(run) {
			ITuple iData = ia.pullDataItem(500);
			if(iData == null) break;
			v1 = iData.getInt("v1");
			v2 = iData.getInt("v2");
			read++;
		}
		System.out.println("v1: " +v1+ " v2: " +v2);
		System.out.println("W: " +written+ " R: " +read);
		assert(written == read);
	}

	/***
	 * Tests that the mechanism to cache a Dataset already in memory to disk 
	 * actually works. A Dataset is populated, then cached to disk. At this 
	 * point the call to consume the next record in the Dataset should return
	 * null (the expected behavior when the Dataset has nothing in memory). If
	 * the Dataset is then uncached and the next item read it should be the
	 * first item in the Dataset.
	 */
//	@Test
	public void testMemToDisk() {
		System.out.println("Testing memory to disk + return");
		//make and populate a new Dataset
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(buildWorkerConfig());
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(((int)System.currentTimeMillis()%Integer.MAX_VALUE), null, null, ServeMode.STORE);
		Dataset testDataset = (Dataset) drm.manageNewDataReference(dataRef);
		for (int x = 0; x < 10; x++) {
			ByteBuffer writeData = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) * 2);
			writeData.putInt((new Integer(Integer.SIZE/Byte.SIZE)));
			writeData.putInt(x);
			testDataset.write(writeData.array(), null);
		}
		//cache dataset and check the caching was successful
		try {
			drm.sendDatasetToDisk(testDataset.id());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(testDataset.consumeData() == null):"Data was consumed from a Dataset residing on disk";
		//uncache dataset and check everything reappeared (and nothing else)
		drm.retrieveDatasetFromDisk(testDataset.id());
		for (int x = 0; x < 10; x++) {
	        byte[] content = testDataset.consumeData();
	        assert(content != null):"Not all data was successfully returned to memory";
			ByteBuffer readData = ByteBuffer.allocate(content.length);
			readData.put(content);
			readData.flip();
			readData.getInt();
			assert(readData.getInt() == x):"The order of the Dataset appears to have been altered by the cache/return process";
		}
		assert(testDataset.consumeData() == null):"New data was generated during the cache/return process";
	}


	/***
	 * Tests that the mechanism to cache a Dataset correctly redirects future
	 * items written to the Dataset to disk. A Dataset is created, cached to
	 * disk, then populated. At this point the call to consume the next record 
	 * in the Dataset should return null (the expected behavior when the Dataset
	 * has nothing in memory). If the Dataset is then uncached and the next item
	 * read it should be the first item in the Dataset.
	 */
//	@Test
	public void testFutureToDisk() {
		System.out.println("Testing read from disk");
		//make, cache, and populate a new Dataset
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(buildWorkerConfig());
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(((int)System.currentTimeMillis()%Integer.MAX_VALUE), null, null, ServeMode.STORE);
		Dataset testDataset = (Dataset) drm.manageNewDataReference(dataRef);
		try {
			drm.sendDatasetToDisk(testDataset.id());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int x = 0; x < 10; x++) {
			ByteBuffer writeData = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) * 2);
			writeData.putInt((new Integer(Integer.SIZE/Byte.SIZE)));
			writeData.putInt(x);
			testDataset.write(writeData.array(), null);
		}
		//check the caching was successful
		int x;
		for (x = 0; x < 5; x++) {
	        byte[] content = testDataset.consumeData();
	        assert(content != null):"Problem reading from disk";
			ByteBuffer readData = ByteBuffer.allocate(content.length);
			readData.put(content);
			readData.flip();
			readData.getInt();
			assert(readData.getInt() == x):"The order of the Dataset appears to have been altered by the cache/return process";
		}
		//uncache dataset and check everything reappeared (and nothing else)
		drm.retrieveDatasetFromDisk(testDataset.id());
		for (; x < 10; x++) {
	        byte[] content = testDataset.consumeData();
	        assert(content != null):"Not all data was successfully returned to memory";
			ByteBuffer readData = ByteBuffer.allocate(content.length);
			readData.put(content);
			readData.flip();
			readData.getInt();
			assert(readData.getInt() == x):"The order of the Dataset appears to have been altered by the cache/return process";
		}
		assert(testDataset.consumeData() == null):"New data was generated during the cache/return process";
	}
}
