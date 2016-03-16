package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.comm.protocol.Command;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class RandomSchedulingStrategy implements SchedulingStrategy{
	
	@Override
	public Stage next(ScheduleTracker tracker, Map<Integer, List<RuntimeEvent>> rEvents) {
		Set<Stage> readySet = tracker.getReadySet();
		return readySet.iterator().next();
	}

	@Override
	public List<Command> postCompletion(Stage finishedStage, ScheduleTracker tracker) {
		return null;
		// TODO Auto-generated method stub
		
	}
	
}
