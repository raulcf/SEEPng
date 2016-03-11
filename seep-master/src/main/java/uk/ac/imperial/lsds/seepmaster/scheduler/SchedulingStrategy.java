package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public interface SchedulingStrategy {

	public Stage next(ScheduleTracker tracker, Map<Integer, List<RuntimeEvent>> rEvents);
	
}
