package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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
	private StageTracker currentStageTracker;
	
	public ScheduleTracker(Set<Stage> stages) {
		status = ScheduleStatus.NON_INITIALIZED;
		// Keep track of overall schedule
		scheduleStatus = new HashMap<>();
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
		}
	}
	
	public Stage getHead(){
		return sink;
	}
	
	public Map<Stage, StageStatus> stages() {
		return scheduleStatus;
	}
	
	public boolean setReady(Stage stage) {
		this.scheduleStatus.put(stage, StageStatus.READY);
		return true;
	}
	
	public boolean setFinished(Stage stage) {
		// Set finish
		this.scheduleStatus.put(stage, StageStatus.FINISHED);
		if(stage.getStageType().equals(StageType.SINK_STAGE)) {
			// Finished schedule
			this.status = ScheduleStatus.FINISHED;
		}
		// Check whether the new stage makes ready new stages
		for(Stage downstream : stage.getDependants()) {
			if(isStageReadyToRun(downstream)) {
				this.scheduleStatus.put(downstream, StageStatus.READY);
			}
		}
		return true;
	}
	
	public boolean isStageReady(Stage stage) {
		return this.scheduleStatus.get(stage).equals(StageStatus.READY);
	}
	
	public boolean isStageWaiting(Stage stage) {
		return this.scheduleStatus.get(stage).equals(StageStatus.WAITING);
	}
	
	public boolean isStageFinished(Stage stage) {
		return this.scheduleStatus.get(stage).equals(StageStatus.FINISHED);
	}
	
	public boolean resetAllStagesTo(StageStatus newStatus) {
		for(Stage st : this.scheduleStatus.keySet()) {
			this.scheduleStatus.put(st, newStatus);
		}
		return true;
	}
	
	private boolean isStageReadyToRun(Stage stage) {
		for(Stage st : stage.getDependencies()) {
			if(! scheduleStatus.get(st).equals(StageStatus.FINISHED)) {
				return false;
			}
		}
		return true;
	}
	
	public void trackAndWait(Stage stage, Set<Integer> euInvolved) {
		currentStageTracker = new StageTracker(stage, euInvolved);
		currentStageTracker.await();
	}

	public void finishStage(int euId, int stageId) {
		currentStageTracker.notifyOk(euId, stageId);
	}
	
}
