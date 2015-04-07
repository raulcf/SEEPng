package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.scheduler.Stage;

public interface SchedulingStrategy {

	public Stage next(ScheduleTracker tracker);
	
}
