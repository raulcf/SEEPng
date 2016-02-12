package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;

public interface ScheduleAPI extends QueryAPI {

	public Stage createStage(int stageId, int opId, StageType t, EndPoint location);
	public boolean declareStages(Stage... stage);
	
}
