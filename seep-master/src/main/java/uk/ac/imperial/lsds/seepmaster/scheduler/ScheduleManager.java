package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;

public interface ScheduleManager {

	public void notifyStageStatus(StageStatusCommand ssc);
	
}
