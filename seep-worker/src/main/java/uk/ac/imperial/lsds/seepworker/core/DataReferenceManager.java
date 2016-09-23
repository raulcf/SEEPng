package uk.ac.imperial.lsds.seepworker.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.OutgoingConnectionRequest;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.core.DatasetMetadataPackage;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.infrastructure.DataEndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.comm.NetworkSelector;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

/**
 * This has to:
 * keep track of all DataReferences that the node manages (store and serve types)
 * keep track of those datasets that correspond to datareferences of type store
 * provide on-demand access to both datasets and datareferences
 * @author ra
 */
public class DataReferenceManager {

	final private Logger LOG = LoggerFactory.getLogger(DataReferenceManager.class.getName());
	private static DataReferenceManager instance;
		
	private Map<Integer, DataReference> catalogue;
	private Map<Integer, Dataset> datasets;
	private List<DataStoreSelector> dataStoreSelectors;
	
	/**
	 * This list keeps datasets ordered by priority of staying in memory. Such order 
	 * is determined by the master and used by DRM to choose which datasets to evict to disk
	 * and which datasets to load from disk.
	 */
	private List<Integer> rankedDatasets;
	
	private DiskCacher cacher;
	
	private int syntheticDatasetGenerator;
	
	private BufferPool bufferPool;
	
	// metrics
	private long __time_freeDatasets = 0;
	
	private DataReferenceManager(WorkerConfig wc) {
		this.catalogue = new HashMap<>();
		this.datasets = new HashMap<>();
		int rnd = new Random().nextInt();
		// Get from WC the data reference ID for the synthetic generator and create a dataset for it
		this.syntheticDatasetGenerator = wc.getInt(WorkerConfig.SYNTHETIC_DATA_GENERATOR_ID) + rnd;
		this.bufferPool = BufferPool.createBufferPool(wc);
		this.cacher = DiskCacher.makeDiskCacher(wc);
	}
	
	public static DataReferenceManager makeDataReferenceManager(WorkerConfig wc) {
		if(instance == null) {
			instance = new DataReferenceManager(wc);
		}
		return instance;
	}
	
	public void updateRankedDatasets(List<Integer> rankedDatasets) {
		
		this.rankedDatasets = rankedDatasets;
		
		freeDatasets();
		//loadToMemoryEvictedDatasets();
		
	}
	
	private void loadToMemoryEvictedDatasets() {
		// Iterate in order until detecting the first dataset not in memory
		for(Integer i : rankedDatasets) {
			if(datasets.containsKey(i)) {
				Dataset d = datasets.get(i);
				if(! cacher.inMem(d)) {
					// Check if there's enough memory to load it back again
					long size = d.size() + bufferPool.getMinimumBufferSize(); // Overcalculate to account for cached buffer
					if(bufferPool.isThereXMemAvailable(size)) {
						this.retrieveDatasetFromDisk(i);
					}
				}
			}
		}
	}
	
	private void freeDatasets() {
		long start = System.currentTimeMillis();
		// Free datasets that are no longer part of the list of rankedDatasets
		int totalFreedMemory = 0;
		Set<Integer> toRemove = new HashSet<>();
		for(Integer dId : datasets.keySet()) {
			if(! rankedDatasets.contains(dId)) {
				// Eliminate dataset
				LOG.info("Marked Dataset for removal: {}", dId);
				totalFreedMemory = totalFreedMemory + datasets.get(dId).freeDataset();
				toRemove.add(dId);
			}
		}
		for (int index = 0; index < rankedDatasets.size(); index++) {
			LOG.info("Dataset {} ranked {}, is in mem? {}", rankedDatasets.get(index), index, datasetIsInMem(rankedDatasets.get(index)));
		}
		for(Integer tr : toRemove){
			datasets.remove(tr);
			catalogue.remove(tr);
		}
		LOG.info("Total freed memory: {}", totalFreedMemory);
		long end = System.currentTimeMillis();
		__time_freeDatasets = __time_freeDatasets + (end - start);
	}
	
	public DatasetMetadataPackage getManagedDatasetsMetadata(Set<Integer> usedSet) {
		Set<DatasetMetadata> oldDatasets = new HashSet<>();
		Set<DatasetMetadata> newDatasets = new HashSet<>();
		Set<DatasetMetadata> usedDatasets = new HashSet<>();
		
		for(Dataset d : this.datasets.values()) { // Iterate over all datasets
			int id = d.id();
			long size = d.size();
			boolean inMem = datasetIsInMem(id);
			long estimatedCreationCost = d.creationCost();
			int diskAccess = d.getDiskAccess();
			if(diskAccess != 0) {
				System.out.println();
			}
			int memAccess = d.getMemAccess();
			DatasetMetadata dm = new DatasetMetadata(id, size, inMem, estimatedCreationCost, diskAccess, memAccess);
			// Classify then as old (non used by this stage) and new (used by this stage)
			if(rankedDatasets.contains(id)) {
				oldDatasets.add(dm);
			}
			else {
				newDatasets.add(dm);
			}
			// Then also add those (repeated reference) that were used by this stage
			if(usedSet.contains(id)) {
				usedDatasets.add(dm);
			}
		}
		double availableMemory = bufferPool.getPercAvailableMemory();
		DatasetMetadataPackage dmp = new DatasetMetadataPackage(oldDatasets, newDatasets, usedDatasets, availableMemory, __time_freeDatasets);
		
		return dmp;
	}
	
	public OBuffer _manageNewDataReferenceBackupOnDisk(DataReference dataRef) {
		int id = dataRef.getId();
		Dataset newDataset = null;
		if(! catalogue.containsKey(id)) {
			LOG.info("Start managing new DataReference, id -> {}", id);
			catalogue.put(id, dataRef);
			// TODO: will become more complex...
			newDataset = Dataset.newDatasetOnDisk(dataRef, bufferPool, this);
			//newDataset = new Dataset(dataRef, bufferPool, this);
			datasets.put(id, newDataset);
		}
		else {
			LOG.warn("Attempt to register an already existent DataReference, id -> {}", id);
		}
		return newDataset;
	}
	
	public OBuffer manageNewDataReference(DataReference dataRef) {
		int id = dataRef.getId();
		Dataset newDataset = null;
		if(! catalogue.containsKey(id)) {
			LOG.info("Start managing new DataReference, id -> {}", id);
			catalogue.put(id, dataRef);
			// TODO: will become more complex...
			newDataset = new Dataset(dataRef, bufferPool, this);
			datasets.put(id, newDataset);
		}
		else {
			LOG.warn("Attempt to register an already existent DataReference, id -> {}", id);
		}
		return newDataset;
	}
	
	public boolean registerDataReferenceInCatalogue(DataReference dr) {
		int drId = dr.getId();
		if( ! catalogue.containsKey(drId)) {
			catalogue.put(drId, dr);
			LOG.info("DataReference id -> {} registered in DRM", drId);
			return true;
		}
		return false;
	}
	
	public DataReference doesManageDataReference(int dataRefId) {
		return catalogue.get(dataRefId);
	}
	
	// FIXME: temporal method
	public void serveDataSet(CoreOutput coreOutput, DataReference dr, DataEndPoint dep) {
		Connection c = new Connection(dep);
		OBuffer buffer = coreOutput.getBuffers().get(dr.getId());
		OutgoingConnectionRequest ocr = new OutgoingConnectionRequest(c, buffer);
		DataStoreType type = dr.getDataStore().type();
		DataStoreSelector dss = getSelectorOfType(dr.getDataStore().type());
		switch(type) {
		case NETWORK:
			Set<OutgoingConnectionRequest> conns = new HashSet<>();
			conns.add(ocr);
			((NetworkSelector)dss).configureOutgoingConnection(conns);
			break;
		default:
			
			break;
		}
	}

	public void setDataStoreSelectors(List<DataStoreSelector> dataStoreSelectors) {
		this.dataStoreSelectors = dataStoreSelectors;
	}
	
	private DataStoreSelector getSelectorOfType(DataStoreType type) {
		for(DataStoreSelector dss : dataStoreSelectors) {
			if(dss.type() == type) return dss;
		}
		return null;
	}
	
	public String createDatasetOnDisk(int datasetId) {
		LOG.info("Creating Dataset on disk, id -> {}", datasetId);
		String name = cacher.createDatasetOnDisk(datasetId);
		LOG.info("Finished caching Dataset to disk, id -> {}", datasetId);
		return name;
	}
	
	public int sendDatasetToDisk(int datasetId) throws IOException {
		LOG.info("Caching Dataset to disk, id -> {}", datasetId);
		int freedMemory = cacher.cacheToDisk(datasets.get(datasetId));
		LOG.info("Cached to disk, id -> {}, freedMemory -> {}", datasetId, freedMemory);
		return freedMemory;
	}
	
	public void retrieveDatasetFromDisk(int datasetId) {
		// Safety check, is there enough memory available
		long memRequired = datasets.get(datasetId).size() + bufferPool.getMinimumBufferSize();
		boolean enoughMem = bufferPool.isThereXMemAvailable(memRequired);
		if(! enoughMem) {
			LOG.error("Impossible to load to memory: Not enough mem available");
			return;
		}
		
		try {
			LOG.info("Returning cached Dataset to memory, id -> {}", datasetId);
			try {
				cacher.retrieveFromDisk(datasets.get(datasetId));
			} 
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		finally {
			LOG.info("Finished returning cached Dataset to memory, id -> {}", datasetId);
		}
	}
	
	public boolean datasetIsInMem(int datasetId) {
		return cacher.inMem(datasets.get(datasetId));
	}

	public IBuffer getInputBufferFor(DataReference dr) {
		// Sanity check
		if(doesManageDataReference(dr.getId()) == null) {
			// TODO: throw error
			LOG.error("Asked to retrieve dataset, but dataset not managed here!");
			System.exit(0);
		}
		return datasets.get(dr.getId());
	}
	
	public IBuffer getSyntheticDataset(DataReference dr, long sizeOfDataToGenerate) {
		Dataset d = new Dataset(dr.getId(), dr, bufferPool, this);
		
		// Store dataset already, in case it needs to be spilled to disk while writing
		// in the below lines
		datasets.put(dr.getId(), d);
		
		Schema s = dr.getDataStore().getSchema();
//		byte[] tuple = OTuple.create(s, s.names(), s.randomValues());
		int size = s.sizeOfTuple();
		byte[] tuple = OTuple.createUnsafe(s.fields(), s.randomValues(), size);
		int tupleSizeWithOverhead = tuple.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
		
		// Filling dataset with data (may or may not spill to disk)
		long numTuples = sizeOfDataToGenerate / tupleSizeWithOverhead;
		int totalWritten = 0;
		OTuple o = new OTuple(s);
		for (int i = 0; i < numTuples; i++) {
//			byte[] srcData = OTuple.create(s, s.names(), s.randomValues());
//			byte[] srcData = OTuple.createUnsafe(s.fields(), s.randomValues(), size);
			o.setValues(s.defaultValues());//s.randomValues());
			totalWritten += o.getTupleSize() + TupleInfo.TUPLE_SIZE_OVERHEAD;
			d.write(o, null);
//			d.write(srcData, null);
		}
		
		LOG.info("Synthetic dataset with {} tuples, size: {}", numTuples, totalWritten);
		
		d.prepareSyntheticDatasetForRead();
//		d.prepareDatasetForFutureRead();
		
		return d;
	}
	
	public IBuffer _getSyntheticDataset(DataReference dr, int sizeOfDataToGenerate) {
		
		ByteBuffer d = ByteBuffer.allocate(sizeOfDataToGenerate);
		
		// Generate synthetic data
		Schema s = dr.getDataStore().getSchema();
		int totalWritten = 0;
		boolean goOn = true;
		int totalTuples = 0;
		while(goOn) {
			byte[] tuple = OTuple.create(s, s.names(), s.randomValues());
			
			if(d.position() + tuple.length + TupleInfo.TUPLE_SIZE_OVERHEAD <= d.capacity()) {
				d.putInt(tuple.length);
				d.put(tuple);
				totalWritten = totalWritten + TupleInfo.TUPLE_SIZE_OVERHEAD + tuple.length;
				totalTuples++;
			}
			else {
				// stop when no more data fits
				goOn = false;
			}
		}
		//Copy only the written bytes
		byte[] dataToForward = new byte[totalWritten];
		System.arraycopy(d.array(), 0, dataToForward, 0, totalWritten);
		LOG.info("Synthetic dataset with {} tuples, size: {}", totalTuples, totalWritten);
		// Store synthetic data in synthetic dataset
		Dataset synthetic = new Dataset(syntheticDatasetGenerator, dataToForward, dr, bufferPool);
		// Store in catalogue and return it for use
		datasets.put(syntheticDatasetGenerator, synthetic);
		return synthetic;
	}
	
//	public List<Integer> spillDatasetsToDisk(Integer datasetId) {
	public int spillDatasetsToDisk(Integer datasetId) {
		LOG.info("Worker node runs out of memory while writing to dataset: {}", datasetId);
		//List<Integer> spilledDatasets = new ArrayList<>();
		int freedMemory = 0;
		
		try {
			if(rankedDatasets == null) {
				if (datasetId != null) {
					freedMemory = sendDatasetToDisk(datasetId);
				}
			}
			else {
				//List<Integer> candidatesToSpill = new ArrayList<>();
				for(Integer i : rankedDatasets) { 
					// We find the first dataset in the list that is in memory and send it to disk
					// TODO: is one enough? how to know?
					if(this.datasetIsInMem(i)) {
						//Dataset candidate = datasets.get(i);
						//candidatesToSpill.add(i);
						freedMemory = sendDatasetToDisk(i);
						//spilledDatasets.add(i);
						if (freedMemory > 0) { //if (candidate.freeDataset() > 0) {
							return freedMemory;
						}
					}
				}
			}
			if (datasetId != null) {
				freedMemory = sendDatasetToDisk(datasetId);
			}
		}
		catch (IOException io) {
			LOG.error("While trying to spill dataset: {} to disk", datasetId);
			io.printStackTrace();
		}
		
		return freedMemory;
	}
	
	public void printCatalogue() {
		for(Entry<Integer, DataReference> entry : catalogue.entrySet()) {
			System.out.println("id: " + entry.getKey()+ " val: " + entry.getValue().getPartitionId());
		}
	}
	
}
