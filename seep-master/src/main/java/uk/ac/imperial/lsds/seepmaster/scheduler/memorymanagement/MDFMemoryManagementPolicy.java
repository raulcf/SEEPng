package uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement;

import java.util.List;
import java.util.Set;

import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class MDFMemoryManagementPolicy implements MemoryManagementPolicy {

	private ScheduleDescription sd;
	private double dmRatio;
	
	public MDFMemoryManagementPolicy(ScheduleDescription sd, double dmRatio) {
		this.sd = sd;
		this.dmRatio = dmRatio;
	}
	
	@Override
	public void updateDatasetsForNode(int euId, Set<DatasetMetadata> datasetsMetadata, int stageId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Integer> rankDatasetsForNode(int euId, Set<Integer> datasetIds) {
		// TODO Auto-generated method stub
		return null;
	}


}
