package uk.ac.imperial.lsds.seepworker.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class DataReferenceManager {

	final private Logger LOG = LoggerFactory.getLogger(DataReferenceManager.class.getName());
	private static DataReferenceManager instance;
		
	private Map<Integer, DataReference> catalogue;
	private Map<DataReference, DataSet> managedDataSets;
	private Executor pool;
	
	private DataReferenceManager(WorkerConfig wc) {
		this.catalogue = new HashMap<>();
		this.managedDataSets = new HashMap<>();
	}
	
	public static DataReferenceManager makeDataReferenceManager(WorkerConfig wc) {
		if(instance == null) {
			instance = new DataReferenceManager(wc);
		}
		return instance;
	}
	
	public void manageNewDataReference(DataReference dataRef) {
		int id = dataRef.getId();
		if(! catalogue.containsKey(id)) {
			LOG.info("Start managing new DataReference, id -> {}", id);
			catalogue.put(id, dataRef);
		}	
		else {
			LOG.warn("Attempt to register an already existent DataReference, id -> {}", id);
		}
	}
	
	public boolean doesManageDataReference(int dataRefId) {
		return catalogue.containsKey(dataRefId);
	}
	
	public void stopManagingDataReference(DataReference dataRef) {
		
	}
	
	public void createNewDataSet(DataReference dataRef) {
		
	}
	
	public void storeDataSet(DataSet dataSet) {
		
	}
	
	public void createAndStoreDataSet(DataSet dataSet) {
		
	}
	
	public void deleteDataSet(DataSet dataSet) {
		
	}
	
	public void serveDataSet(EndPoint ep) {
		
	}
	
}
