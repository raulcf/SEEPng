package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;

public interface ScheduleAPI extends QueryAPI {

	public Stage createStage(int stageId, int opId, StageType t, ControlEndPoint location);
	public boolean declareStages(Stage... stage);
	
}
