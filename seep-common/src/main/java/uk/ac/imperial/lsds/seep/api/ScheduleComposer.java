package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public interface ScheduleComposer {

	public static ScheduleBuilder schedAPI = new ScheduleBuilder();
	
	public ScheduleDescription compose();
	
}
