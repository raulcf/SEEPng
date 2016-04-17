package uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import uk.ac.imperial.lsds.seep.core.DatasetMetadata;

public class SizeObliviousLRUMemoryManagementPolicy implements MemoryManagementPolicy {

	/**
	 * A map that given an id, returns a monotonically increasing id that represents the last time the dataset was used,
	 * with lower numbers meaning older.
	 * Indexed by EU_ID first
	 */	
	private Map<Integer, Map<Integer, Integer>> euId_lru;
	
	public SizeObliviousLRUMemoryManagementPolicy() {
		euId_lru = new HashMap<>();
	}
	
	@Override
	public void updateDatasetsForNode(int euId, Set<DatasetMetadata> datasetsMetadata) {
		for(DatasetMetadata datasetMetadata : datasetsMetadata) {
			int datasetId = datasetMetadata.getDatasetId();
			touchDataset(euId, datasetId);
		}
	}
	
	private void touchDataset(int euId, int datasetId) {
		// Make sure we have an entry for the node
		if(! euId_lru.containsKey(euId)) {
			euId_lru.put(euId, new HashMap<>());
		}
		
		Map<Integer, Integer> datasetId_timestamp = euId_lru.get(euId);
		
		// Then check whether we have an entry for the datasetId
		if(! datasetId_timestamp.containsKey(datasetId)) {
			datasetId_timestamp.put(datasetId, 0);
		}
		int currentTimestamp = datasetId_timestamp.get(datasetId);
		datasetId_timestamp.put(datasetId, (currentTimestamp + 1)); // touching means increment timestamp
	}

	@Override
	public List<Integer> rankDatasetsForNode(int euId, Set<Integer> datasetIds) {
		List<Integer> rankedDatasets = new ArrayList<>();
		if(! euId_lru.containsKey(euId)) {
			return rankedDatasets;
		}
		Map<Integer, Integer> datasetId_timestamp = euId_lru.get(euId);
		
		Map<Integer, Integer> sorted = sortByValue(datasetId_timestamp);
		// TODO: may break ordering due to keyset returning a set ?
		for(Integer key : sorted.keySet()) {
			rankedDatasets.add(key);
		}
		return rankedDatasets;
	}
	
	private Map<Integer, Integer> sortByValue( Map<Integer, Integer> map ) {
		Map<Integer, Integer> result = new LinkedHashMap<>();
		Stream <Entry<Integer, Integer>> st = map.entrySet().stream();
		st.sorted(Comparator.comparing(e -> e.getValue())).forEachOrdered(e -> result.put(e.getKey(),e.getValue()));
		return result;
	}

}
