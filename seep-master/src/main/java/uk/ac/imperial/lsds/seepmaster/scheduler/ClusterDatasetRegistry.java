package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement.MemoryManagementPolicy;


public class ClusterDatasetRegistry {

	// The class that implements the memory management policy, used to rank datasets in nodes
	private MemoryManagementPolicy mmp;
	
	// Keeps a map from SeepEndPoint id to the list of datasetsMetadata managed there
	private Map<Integer, Set<DatasetMetadata>> datasetsPerNode;
	
	// Keeps a map of datasets per node ordered by priority to live in memory
	private Map<Integer, List<Integer>> rankedDatasetsPerNode;
	
	// Metrics
	
	private int totalDatasetsGenerated = 0;
	private int totalDatasetsSpilledToDisk = 0;
	
	public int totalDatasetsGeneratedDuringSchedule() {
		return this.totalDatasetsGenerated;
	}
	
	public int totalDatasetsSpilledToDiskDuringSchedule() {
		return this.totalDatasetsSpilledToDisk;
	}
	
	public ClusterDatasetRegistry(MemoryManagementPolicy mmp) {
		this.datasetsPerNode = new HashMap<>();
		this.rankedDatasetsPerNode = new HashMap<>();
		this.mmp = mmp;
	}
	
	public int totalDatasetsInCluster() {
		int total = 0;
		for (Set<DatasetMetadata> list : datasetsPerNode.values()) {
			total = total + list.size();
		}
		return total;
	}
	
	public Set<Integer> getDatasetIdsForNode(int euId) {
		Set<Integer> datasets = new HashSet<>();
		if(datasetsPerNode.containsKey(euId)) {
			for(DatasetMetadata dm : datasetsPerNode.get(euId)) {
				datasets.add(dm.getDatasetId());
			}
		}
		return datasets;
	}
	
	public int totalDatasetsInSeepEndPoint(int euId) {
		return this.datasetsPerNode.get(euId).size();
	}

	public void updateDatasetsForNode(int euId, Set<DatasetMetadata> managedDatasets) {
		// Update metrics
		updateMetrics(managedDatasets);
		mmp.updateDatasetsForNode(euId, managedDatasets);
		this.datasetsPerNode.put(euId, managedDatasets);
	}

	public List<Integer> getRankedDatasetForNode(int euId, ScheduleDescription schedDesc) {
		this.rankDatasets(euId); // eagerly rerank if necessary before returning the (potentially) new order
		return rankedDatasetsPerNode.get(euId);
	}
	
	private void rankDatasets(int euId) {
		List<Integer> rankedDatasetsForNode = mmp.rankDatasetsForNode(euId, getDatasetIdsForNode(euId));
		this.rankedDatasetsPerNode.put(euId, rankedDatasetsForNode);
	}

	public void evictDatasetFromCluster(int id) {
		for(Entry<Integer, Set<DatasetMetadata>> entry : datasetsPerNode.entrySet()) {
			int key = entry.getKey();
			datasetsPerNode.get(key).remove(id);		
		}
	}
	
	private void updateMetrics(Set<DatasetMetadata> managedDatasets) {
		for(DatasetMetadata dm : managedDatasets) {
			totalDatasetsGenerated++;
			if(dm.isInMem()) {
				totalDatasetsSpilledToDisk++;
			}
		}
	}
	
}
