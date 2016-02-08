package uk.ac.imperial.lsds.seep.scheduler.engine;

import uk.ac.imperial.lsds.seep.scheduler.Stage;

public interface SchedulingStrategy {

	public Stage next(ScheduleTracker tracker);
	
}
