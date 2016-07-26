package uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.core.DatasetMetadataPackage;

public class SizeObliviousLRUMemoryManagementPolicy implements MemoryManagementPolicy {

	/**
	 * A map that given an id, returns a monotonically increasing id that represents the last time the dataset was used,
	 * with lower numbers meaning older.
	 * Indexed by EU_ID first
	 */	
	private Map<Integer, Map<Integer, Integer>> euId_lru;
	
	private int maxTimestamp = 0;
	
	// Metrics
	private long __totalUpdateTime = 0;
	private long __totalRankTime = 0;
	
	public SizeObliviousLRUMemoryManagementPolicy() {
		euId_lru = new HashMap<>();
	}
	
	@Override
	public void updateDatasetsForNode(int euId, DatasetMetadataPackage datasetsMetadata, int stageId) {
		long start = System.currentTimeMillis();
		Set<DatasetMetadata> usedDatasets = datasetsMetadata.usedDatasets;
		Set<DatasetMetadata> allDatasets = datasetsMetadata.oldDatasets;
		allDatasets.addAll(datasetsMetadata.newDatasets);
		registerDatasets(euId, allDatasets);
		for(DatasetMetadata datasetMetadata : usedDatasets) {
			int datasetId = datasetMetadata.getDatasetId();
			touchDataset(euId, datasetId);
		}
		long end = System.currentTimeMillis();
		this.__totalUpdateTime = this.__totalUpdateTime + (end - start);
	}
	
	private void registerDatasets(int euId, Set<DatasetMetadata> allDatasets) {
		// Make sure we have an entry for the node
		if(! euId_lru.containsKey(euId)) {
			euId_lru.put(euId, new HashMap<>());
		}
		
		Map<Integer, Integer> datasetId_timestamp = euId_lru.get(euId);
		
		// Then check whether we have an entry for the datasetId
		for(DatasetMetadata dm : allDatasets) {
			int did = dm.getDatasetId();
			if(! datasetId_timestamp.containsKey(did)) { // If it does not exist we register with maxTimestamp -> fresh in mem
				datasetId_timestamp.put(did, maxTimestamp);
			}
		}
	}
	
	private void touchDataset(int euId, int datasetId) {
//		// Make sure we have an entry for the node
//		if(! euId_lru.containsKey(euId)) {
//			euId_lru.put(euId, new HashMap<>());
//		}
//		
		Map<Integer, Integer> datasetId_timestamp = euId_lru.get(euId);
//		
//		// Then check whether we have an entry for the datasetId
//		if(! datasetId_timestamp.containsKey(datasetId)) {
//			datasetId_timestamp.put(datasetId, 0);
//		}
		int currentTimestamp = datasetId_timestamp.get(datasetId);
		int newTimestamp = currentTimestamp + 1;
		datasetId_timestamp.put(datasetId, newTimestamp); // touching means increment timestamp
		if (newTimestamp > maxTimestamp) maxTimestamp = newTimestamp;
	}

	@Override
	public List<Integer> rankDatasetsForNode(int euId, Set<Integer> datasetIds) {
		long start = System.currentTimeMillis();
		List<Integer> rankedDatasets = new ArrayList<>();
		if(! euId_lru.containsKey(euId)) {
			return rankedDatasets;
		}
		
		// Now we use the datasets that are alive to prune the datasets in the node
		removeEvictedDatasets(euId, datasetIds);
		
		// We get the datasets in the node, after pruning
		Map<Integer, Integer> datasetId_timestamp = euId_lru.get(euId);
		
		Map<Integer, Integer> sorted = sortByValue(datasetId_timestamp);
//		System.out.println("LRU VALUES");
//		for(Integer v : sorted.values()) {
//			System.out.print(v+" - ");
//		}
//		System.out.println();
		
		// TODO: may break ordering due to keyset returning a set ?
		for(Integer key : sorted.keySet()) {
			rankedDatasets.add(key);
		}
		long end = System.currentTimeMillis();
		this.__totalRankTime = this.__totalRankTime + (end - start);
		return rankedDatasets;
	}
	
	private void removeEvictedDatasets(int euId, Set<Integer> datasetIdsToKeep) {
		Map<Integer, Integer> allEntries = euId_lru.get(euId);
		
		// Select entries to remove
		Set<Integer> toRemove = new HashSet<>();
		for(int id : allEntries.keySet()) {
			if(! datasetIdsToKeep.contains(id)) {
				toRemove.add(id);
			}
		}
		
		// Remove the selection
		for(int toRem : toRemove) {
			allEntries.remove(toRem);
		}
		
		// Update the info
		euId_lru.put(euId, allEntries);
	}
	
	private Map<Integer, Integer> sortByValue( Map<Integer, Integer> map ) {
		Map<Integer, Integer> result = new LinkedHashMap<>();
		Stream <Entry<Integer, Integer>> st = map.entrySet().stream();
		st.sorted(Comparator.comparing(e -> e.getValue())).forEachOrdered(e -> result.put(e.getKey(),e.getValue()));
		return result;
	}

	@Override
	public long __totalUpdateTime() {
		return this.__totalUpdateTime;
	}

	@Override
	public long __totalRankTime() {
		return this.__totalRankTime;
	}

}
