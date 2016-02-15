package uk.ac.imperial.lsds.seepworker.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 *
 */
public class DataReferenceManager {

	final private Logger LOG = LoggerFactory.getLogger(DataReferenceManager.class.getName());
	private static DataReferenceManager instance;
		
	private Map<Integer, DataReference> catalogue;
	private Map<Integer, Dataset> datasets;
	private List<DataStoreSelector> dataStoreSelectors;
	
	private int syntheticDatasetGenerator;
	
	private BufferPool bufferPool;
	
	private DataReferenceManager(WorkerConfig wc) {
		this.catalogue = new HashMap<>();
		this.datasets = new HashMap<>();
		// Get from WC the data reference ID for the synthetic generator and create a dataset for it
		this.syntheticDatasetGenerator = wc.getInt(WorkerConfig.SYNTHETIC_DATA_GENERATOR_ID);
		this.bufferPool = BufferPool.createBufferPool(wc);
	}
	
	public static DataReferenceManager makeDataReferenceManager(WorkerConfig wc) {
		if(instance == null) {
			instance = new DataReferenceManager(wc);
		}
		return instance;
	}
	
	public OBuffer manageNewDataReference(DataReference dataRef) {
		int id = dataRef.getId();
		Dataset newDataset = null;
		if(! catalogue.containsKey(id)) {
			LOG.info("Start managing new DataReference, id -> {}", id);
			catalogue.put(id, dataRef);
			// TODO: will become more complex...
			newDataset = new Dataset(dataRef, bufferPool);
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
	
	public void sendDatasetToDisk(int datasetId) throws IOException {
		LOG.info("Caching Dataset to disk, id -> {}", datasetId);
		datasets.get(datasetId).cacheToDisk();
		LOG.info("Finished caching Dataset to disk, id -> {}", datasetId);
	}
	
	public int retrieveDatasetFromDisk(int datasetId) {
		try {
			LOG.info("Returning cached Dataset to memory, id -> {}", datasetId);
			return datasets.get(datasetId).retrieveFromDisk();
		} finally {
			LOG.info("Finished returning cached Dataset to memory, id -> {}", datasetId);
		}
	}
	
	public boolean datasetIsInMem(int datasetId) {
		return datasets.get(datasetId).inMem();
	}

	public IBuffer getInputBufferFor(DataReference dr) {
		// Sanity check
		if(doesManageDataReference(dr.getId()) == null) {
			// TODO: throw error
		}
		return datasets.get(dr.getId());
	}
	
	public IBuffer getSyntheticDataset(DataReference dr) {
		
		// TODO: basic generation of data
		ByteBuffer d = ByteBuffer.allocate(3999);
		
		// Generate synthetic data
		Schema s = dr.getDataStore().getSchema();
		int totalWritten = 0;
		boolean goOn = true;
		int totalTuples = 0;
		while(goOn) {
			byte[] tuple = OTuple.create(s, s.names(), s.defaultValues());
			
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
	
}
