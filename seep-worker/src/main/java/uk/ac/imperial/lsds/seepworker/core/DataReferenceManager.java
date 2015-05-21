package uk.ac.imperial.lsds.seepworker.core;

import java.util.Map;
import java.util.concurrent.Executor;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public class DataReferenceManager {

	private Map<Integer, DataReference> catalogue;
	private Map<DataReference, DataSet> managedDataSets;
	private Executor pool;
	
	public void manageNewDataReference(DataReference dataRef) {
		
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
