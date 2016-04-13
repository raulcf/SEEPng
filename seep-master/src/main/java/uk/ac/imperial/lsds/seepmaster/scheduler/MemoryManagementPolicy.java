package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.List;
import java.util.Set;

public interface MemoryManagementPolicy {

	public void updateDatasetsForNode(int euId, Set<Integer> datasetIds);
	public List<Integer> rankDatasetsForNode(int euId, Set<Integer> datasetIds);
	
}
