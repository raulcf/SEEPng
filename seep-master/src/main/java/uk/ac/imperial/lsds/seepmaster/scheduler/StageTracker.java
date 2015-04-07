package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.scheduler.StageType;

public class StageTracker {

	private final int stageId;
	private final StageType stageType;
	
	public StageTracker(int stageId, StageType stageType) {
		this.stageId = stageId;
		this.stageType = stageType;
	}

}
