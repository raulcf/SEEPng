package uk.ac.imperial.lsds.seepmaster.scheduler.schedulingstrategy;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.comm.protocol.Command;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepmaster.scheduler.ScheduleTracker;

public class SequentialSchedulingStrategy implements SchedulingStrategy {
	
	@Override
	public Stage next(ScheduleTracker tracker, Map<Integer, List<RuntimeEvent>> rEvents) {
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

	@Override
	public List<Command> postCompletion(Stage finishedStage, ScheduleTracker tracker) {
		return null;
		// TODO Auto-generated method stub
		
	}
}
