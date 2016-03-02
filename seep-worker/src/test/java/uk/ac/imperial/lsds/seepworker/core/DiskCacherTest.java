package uk.ac.imperial.lsds.seepworker.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class DiskCacherTest {
	Schema s = Schema.SchemaBuilder.getInstance().newField(Type.INT, "counter").build();
	int ownerCounter = 0;

	private WorkerConfig buildWorkerConfig() {
		Properties p = new Properties();
		p.setProperty(WorkerConfig.MASTER_IP, "");
		p.setProperty(WorkerConfig.PROPERTIES_FILE, "");
		p.setProperty(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH, "100");
		p.setProperty(WorkerConfig.LISTENING_IP, "");
		
		return new WorkerConfig(p);
	}

	/***
	 * Simple test that creates a Dataset attached to a DataReferenceManager, 
	 * then checks that said DataReferenceManager successfully tracks when the
	 * Dataset resides in memory and when it has been pushed to disk.
	 */
	@Test
	public void testInMemCall() {
		//make new Dataset
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(buildWorkerConfig());
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(ownerCounter++, null, null, ServeMode.STORE);
		Dataset testDataset = (Dataset) drm.manageNewDataReference(dataRef);
		//new Datasets should reside in memory.
		assert(drm.datasetIsInMem(testDataset.id()) == true);
		//cache the Dataset and check that the in memory tracker is updated
		try {
			drm.sendDatasetToDisk(testDataset.id());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(drm.datasetIsInMem(testDataset.id()) == false);
		//return the Dataset to memory and check that the in memory tracker is updated again
		drm.retrieveDatasetFromDisk(testDataset.id());
		assert(drm.datasetIsInMem(testDataset.id()) == true);
	}

	/***
	 * Tests that the mechanism to cache a Dataset already in memory to disk 
	 * actually works. A Dataset is populated, then cached to disk. At this 
	 * point the call to consume the next record in the Dataset should return
	 * null (the expected behavior when the Dataset has nothing in memory). If
	 * the Dataset is then uncached and the next item read it should be the
	 * first item in the Dataset.
	 */
	@Test
	public void testMemToDisk() {
		//make and populate a new Dataset
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(buildWorkerConfig());
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(ownerCounter++, null, null, ServeMode.STORE);
		Dataset testDataset = (Dataset) drm.manageNewDataReference(dataRef);
		for (int x = 0; x < 10; x++) {
			ByteBuffer writeData = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) * 2);
			writeData.putInt((new Integer(Integer.SIZE/Byte.SIZE)));
			writeData.putInt(x);
			testDataset.write(writeData.array());
		}
		//cache dataset and check the caching was successful
		try {
			drm.sendDatasetToDisk(testDataset.id());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assert(testDataset.consumeData() == null);
		//uncache dataset and check everything reappeared (and nothing else)
		drm.retrieveDatasetFromDisk(testDataset.id());
		for (int x = 0; x < 10; x++) {
			ByteBuffer readData = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) * 2);
			readData.put(testDataset.consumeData());
			readData.getInt();
			assert(readData.getInt() == x);
		}
		assert(testDataset.consumeData() == null);
	}


	/***
	 * Tests that the mechanism to cache a Dataset correctly redirects future
	 * items written to the Dataset to disk. A Dataset is created, cached to
	 * disk, then populated. At this point the call to consume the next record 
	 * in the Dataset should return null (the expected behavior when the Dataset
	 * has nothing in memory). If the Dataset is then uncached and the next item
	 * read it should be the first item in the Dataset.
	 */
	@Test
	public void testFutureToDisk() {
		//make, cache, and populate a new Dataset
		DataReferenceManager drm = DataReferenceManager.makeDataReferenceManager(buildWorkerConfig());
		DataReference dataRef = DataReference.makeManagedDataReferenceWithOwner(ownerCounter++, null, null, ServeMode.STORE);
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
			testDataset.write(writeData.array());
		}
		//check the caching was successful
		assert(testDataset.consumeData() == null);
		//uncache dataset and check everything reappeared (and nothing else)
		drm.retrieveDatasetFromDisk(testDataset.id());
		for (int x = 0; x < 10; x++) {
			ByteBuffer readData = ByteBuffer.allocate((Integer.SIZE/Byte.SIZE) * 2);
			readData.put(testDataset.consumeData());
			readData.getInt();
			assert(readData.getInt() == x);
		}
		assert(testDataset.consumeData() == null);
	}

}
