package uk.ac.imperial.lsds.seepmaster.scheduler.schedulingstrategy;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.comm.protocol.Command;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepmaster.scheduler.ScheduleTracker;

public interface SchedulingStrategy {

	public Stage next(ScheduleTracker tracker, Map<Integer, List<RuntimeEvent>> rEvents);
	
	public List<Command> postCompletion(Stage finishedStage, ScheduleTracker tracker);
	
}
