package uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement;

import java.util.List;
import java.util.Set;

import uk.ac.imperial.lsds.seep.core.DatasetMetadata;

public interface MemoryManagementPolicy {

	public void updateDatasetsForNode(int euId, Set<DatasetMetadata> datasetIds, int stageId);
	public List<Integer> rankDatasetsForNode(int euId, Set<Integer> datasetIds);
	
}
