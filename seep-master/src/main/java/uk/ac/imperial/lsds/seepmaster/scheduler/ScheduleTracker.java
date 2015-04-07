package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;
import uk.ac.imperial.lsds.seep.scheduler.StageType;

public class ScheduleTracker {

	final private Logger LOG = LoggerFactory.getLogger(ScheduleTracker.class);
	
	private ScheduleStatus status;
	private Stage sink;
	private Map<Stage, StageStatus> scheduleStatus;
	private Map<Stage, StageTracker> stageTracker;
	
	public ScheduleTracker(Set<Stage> stages) {
		status = ScheduleStatus.NON_INITIALIZED;
		// Keep track of overall schedule
		scheduleStatus = new HashMap<>();
		stageTracker = new HashMap<>();
		for(Stage stage : stages) {
			if(stage.getStageId() == 0) {
				// sanity check
				if(! stage.getStageType().equals(StageType.SINK_STAGE)){
					LOG.error("Stage 0 is not SINK_STAGE !!");
					// FIXME: throw a proper exception instead
					System.exit(0);
				}
				sink = stage;
			}
			scheduleStatus.put(stage, StageStatus.WAITING);
			stageTracker.put(stage, new StageTracker(stage.getStageId(), stage.getStageType()));
		}
	}
	
	public Stage getHead(){
		return sink;
	}
	
	public Map<Stage, StageStatus> stages() {
		return scheduleStatus;
	}
	
}
