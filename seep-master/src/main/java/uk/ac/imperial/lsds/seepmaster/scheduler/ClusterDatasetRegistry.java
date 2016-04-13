package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ClusterDatasetRegistry {

	// The class that implements the memory management policy, used to rank datasets in nodes
	private MemoryManagementPolicy mmp;
	
	// Keeps a map from SeepEndPoint id to the list of datasets managed there
	private Map<Integer, Set<Integer>> datasetsPerNode;
	
	// Keeps a map of datasets per node ordered by priority to live in memory
	private Map<Integer, List<Integer>> rankedDatasetsPerNode;
	
	public ClusterDatasetRegistry(MemoryManagementPolicy mmp) {
		this.datasetsPerNode = new HashMap<>();
		this.rankedDatasetsPerNode = new HashMap<>();
	}
	
	public int totalDatasetsInCluster() {
		int total = 0;
		for (Set<Integer> list : datasetsPerNode.values()) {
			total = total + list.size();
		}
		return total;
	}
	
	public int totalDatasetsInSeepEndPoint(int euId) {
		return this.datasetsPerNode.get(euId).size();
	}

	public void updateDatasetsForNode(int euId, Set<Integer> managedDatasets) {
		mmp.updateDatasetsForNode(euId, managedDatasets);
		this.datasetsPerNode.put(euId, managedDatasets);
	}

	public List<Integer> getRankedDatasetForNode(int euId) {
		this.rankDatasets(); // eagerly rerank if necessary before returning the (potentially) new order
		return rankedDatasetsPerNode.get(euId);
	}
	
	private void rankDatasets() {
		// TODO: use mmp to rank datasets
		
	}
	
}
