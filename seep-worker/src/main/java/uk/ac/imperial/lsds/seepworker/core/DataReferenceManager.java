package uk.ac.imperial.lsds.seepworker.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.OutgoingConnectionRequest;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.comm.NetworkSelector;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

public class DataReferenceManager {

	final private Logger LOG = LoggerFactory.getLogger(DataReferenceManager.class.getName());
	private static DataReferenceManager instance;
		
	private Map<Integer, DataReference> catalogue;
	private Map<DataReference, DataSet> managedDataSets;
	private Executor pool;
	private List<DataStoreSelector> dataStoreSelectors;
	
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
	
	public DataReference doesManageDataReference(int dataRefId) {
		return catalogue.get(dataRefId);
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
	
	// FIXME: temporal method
	public void serveDataSet(CoreOutput coreOutput, DataReference dr, EndPoint ep) {
		Connection c = new Connection(ep);
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
	
}
