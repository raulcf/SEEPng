package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.scheduler.Stage;

public interface ScheduleAPI extends QueryAPI {

	public boolean declareStages(Stage... stage);
	
}
