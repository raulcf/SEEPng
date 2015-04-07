package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.Map;

import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;

public class SequentialSchedulingStrategy implements SchedulingStrategy {

	@Override
	public Stage next(ScheduleTracker tracker) {
		// Explore from stage 0 (the sink) and backwards until detect the next stage to execute
		Map<Stage, StageStatus> stages = tracker.stages();
		Stage head = tracker.getHead();
		Stage nextToSchedule = nextStageToSchedule(head, stages);
		return nextToSchedule;
	}
	
	private Stage nextStageToSchedule(Stage head, Map<Stage, StageStatus> stages){
		Stage next = head;
		for(Stage upstream : head.getDependencies()) {
			// any failed stage should have been handled already before
			if(stages.get(upstream).equals(StageStatus.WAITING)){
				return nextStageToSchedule(upstream, stages);
			}
		}
		return next;
	}

}
