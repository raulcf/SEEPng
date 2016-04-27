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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class MDFMemoryManagementPolicy implements MemoryManagementPolicy {

	final private Logger LOG = LoggerFactory.getLogger(MDFMemoryManagementPolicy.class);
	
	private ScheduleDescription sd;
	private double dmRatio;
	
	private Map<Integer, Integer> stageid_accesses = new HashMap<>();
	private Map<Integer, Long> stageid_size = new HashMap<>();
	private Map<Integer, Long> stageid_cost = new HashMap<>();
	private Map<Integer, Double> stageid_ratio_inmem = new HashMap<>();
	
	private Map<Integer, Map<Integer, Double>> nodeid_stageid_precedence = new HashMap<>();
	
	public MDFMemoryManagementPolicy(ScheduleDescription sd, double dmRatio) {
		this.sd = sd;
		this.dmRatio = dmRatio;
		computeAccesses(sd);
	}
	
	@Override
	public void updateDatasetsForNode(int euId, Set<DatasetMetadata> datasetsMetadata, int stageId) {
		// Compute variables for the model
		long sizeOfThisDataset = computeSizeOfDataset(datasetsMetadata);
		long costOfDataset = computeCostOfDataset(datasetsMetadata);
		double percDataInMem = computeRatioDataInMem(datasetsMetadata, sizeOfThisDataset);
		int accesses = stageid_accesses.get(stageId);
		
		// Store variables
		stageid_size.put(stageId, sizeOfThisDataset);
		stageid_cost.put(stageId, costOfDataset);
		stageid_ratio_inmem.put(stageId, percDataInMem);
		
		double IOCostForThisDataset = sizeOfThisDataset * dmRatio;
		double recomputeCost = computeRecomputeCostFor(stageId);
		
		double factor = Math.min(IOCostForThisDataset, recomputeCost);
		
		double precedence = accesses * factor;
		nodeid_stageid_precedence.get(euId).put(stageId, precedence);
	}
	
	private double computeRecomputeCostFor(int stageId) {
		double recomputeCost = Long.MAX_VALUE;
		Stage s = sd.getStageWithId(stageId);
		Set<Stage> upstream = s.getDependencies();
		if(upstream.size() != 1) {
			LOG.error("upstream of more than 1 when computing recompute cost for stageId: {}" ,stageId);
		}
		int sid = upstream.iterator().next().getStageId();
		if(! stageid_size.containsKey(stageId)) {
			return recomputeCost; // make sure this is not selected, as it does not exist yet
		}
		long size = stageid_size.get(sid);
		long cost = stageid_cost.get(sid);
		double percDataInMem = stageid_ratio_inmem.get(sid);
		
		recomputeCost = cost + percDataInMem * (size * dmRatio);
		
		return recomputeCost;
	}
	
	private long computeSizeOfDataset(Set<DatasetMetadata> datasetsMetadata) {
		long size = 0;
		for(DatasetMetadata dm : datasetsMetadata) {
			size = size + dm.getSize();
		}
		return size;
	}
	
	private long computeCostOfDataset(Set<DatasetMetadata> datasetsMetadata) {
		long cost = 0;
		for(DatasetMetadata dm : datasetsMetadata) {
			cost = cost + dm.getCreationCost();
		}
		return cost;
	}
	
	private double computeRatioDataInMem(Set<DatasetMetadata> datasetsMetadata, long sizeOfThisDataset) {
		// doing this on actual size in case datasets are of different lenghts in the future
		double r = 0;
		long mem = 0;
		for(DatasetMetadata dm : datasetsMetadata) {
			if(dm.isInMem()) {
				mem = mem + dm.getSize();
			}
		}
		r = mem/sizeOfThisDataset;
		return r;
	}

	@Override
	public List<Integer> rankDatasetsForNode(int euId, Set<Integer> datasetIds) {
		List<Integer> rankedDatasets = new ArrayList<>();
		if(! nodeid_stageid_precedence.containsKey(euId)) {
			return rankedDatasets;
		}
		
		// Now we use the datasets that are alive to prune the datasets in the node
		removeEvictedDatasets(euId, datasetIds);
		
		// We get the datasets in the node, after pruning
		Map<Integer, Double> datasetId_timestamp = nodeid_stageid_precedence.get(euId);
		
		Map<Integer, Double> sorted = sortByValue(datasetId_timestamp);
		// TODO: may break ordering due to keyset returning a set ?
		for(Integer key : sorted.keySet()) {
			rankedDatasets.add(key);
		}
		return rankedDatasets;
	}
	
	private void computeAccesses(ScheduleDescription sd) {
		// TODO: will work if there is only one (logical) source
		for(Stage s : sd.getStages()) {
			int sid = s.getStageId();
			int numUpstream = s.getDependencies().size();
			stageid_accesses.put(sid, numUpstream);
		}
	}
	
	private void removeEvictedDatasets(int euId, Set<Integer> datasetIdsToKeep) {
		Map<Integer, Double> allEntries = nodeid_stageid_precedence.get(euId);
		
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
		nodeid_stageid_precedence.put(euId, allEntries);
	}
	
	private Map<Integer, Double> sortByValue( Map<Integer, Double> map ) {
		Map<Integer, Double> result = new LinkedHashMap<>();
		Stream <Entry<Integer, Double>> st = map.entrySet().stream();
		st.sorted(Comparator.comparing(e -> e.getValue())).forEachOrdered(e -> result.put(e.getKey(),e.getValue()));
		return result;
	}

}
