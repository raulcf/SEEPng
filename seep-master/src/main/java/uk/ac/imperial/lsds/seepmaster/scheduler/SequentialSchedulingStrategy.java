package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class SequentialSchedulingStrategy implements SchedulingStrategy {

	@Override
	public Stage next(ScheduleTracker tracker) {
		// Explore from stage 0 (the sink) and backwards until detect the next stage to execute
		Stage head = tracker.getHead();
		Stage nextToSchedule = nextStageToSchedule(head, tracker);
		return nextToSchedule;
	}
	
	private Stage nextStageToSchedule(Stage head, ScheduleTracker tracker) {
		Stage toReturn = null;
		if(tracker.isStageReady(head)) {
			toReturn = head;
		}
		else {
			for(Stage upstream : head.getDependencies()) {
				if(! tracker.isStageFinished(upstream)) {
					toReturn = nextStageToSchedule(upstream, tracker);
				}
			}
		}
		return toReturn;
	}
}
