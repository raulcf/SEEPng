package uk.ac.imperial.lsds.seep.scheduler.engine;

import java.util.Set;

import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class RandomSchedulingStrategy implements SchedulingStrategy{
	
	@Override
	public Stage next(ScheduleTracker tracker) {
		Set<Stage> readySet = tracker.getReadySet();
		return readySet.iterator().next();
	}
	
}
