package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement.MemoryManagementPolicy;

public class ScheduleTracker {

	final private Logger LOG = LoggerFactory.getLogger(ScheduleTracker.class);
	
	private ScheduleDescription scheduleDescription;
	private Set<Stage> stages;
	private ScheduleStatus status;
	private Stage sink;
	private Map<Stage, StageStatus> scheduleStatus;
	private StageTracker currentStageTracker;
	
	// The registry of all the datasets in the cluster
	private ClusterDatasetRegistry clusterDatasetRegistry;
	
	// RuntimeEvents piggybacked with the status of the last stage executed
	private boolean runtimeEventsInLastStageExecution = false;
	private Map<Integer, List<RuntimeEvent>> lastStageRuntimeEvents = null;
	
	public ScheduleTracker(ScheduleDescription scheduleDescription, MemoryManagementPolicy mmp) {
		this.scheduleDescription = scheduleDescription;
		this.stages = this.scheduleDescription.getStages();
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
		this.lastStageRuntimeEvents = new HashMap<>();
		this.clusterDatasetRegistry = new ClusterDatasetRegistry(mmp);
	}
	
	public ScheduleDescription getScheduleDescription() {
		return scheduleDescription;
	}
 	
	public ClusterDatasetRegistry getClusterDatasetRegistry() {
		return clusterDatasetRegistry;
	}
	
	public boolean didLastStageGenerateRuntimeEvents() {
		return this.runtimeEventsInLastStageExecution;
	}
	
	public Map<Integer, List<RuntimeEvent>> getRuntimeEventsOfLastStageExecution() {
		return this.lastStageRuntimeEvents;
	}
	
	public boolean isScheduledFinished() {
		return this.status == ScheduleStatus.FINISHED;
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
	
	public Set<Stage> getReadySet() {
		Set<Stage> toReturn = new HashSet<>();
		for(Stage stage : stages) {
			if(this.isStageReady(stage)) {
				toReturn.add(stage);
			}
		}
		return toReturn;
	}
	
	public boolean setFinished(Stage stage, Map<Integer, Set<DataReference>> results) {
		// results contains the DataReferences for downstream stages, indexed by
		// downstream stage id
		LOG.info("[FINISH] SCHEDULING Stage {}", stage.getStageId());
		// Set finish
		this.scheduleStatus.put(stage, StageStatus.FINISHED);
		if(stage.getStageType().equals(StageType.SINK_STAGE)) {
			// Finished schedule
			this.status = ScheduleStatus.FINISHED;
		}
		// Check whether the new stage makes ready new stages, and propagate results
		for(Stage downstream : stage.getDependants()) {
			// Results for this stage 
			Set<DataReference> resultsForThisStage = results.get(stage.getStageId());
			if(resultsForThisStage != null) { // TODO: if the task produced results, add them
				downstream.addInputDataReference(stage.getStageId(), resultsForThisStage);
			}
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
	
	public void trackWorkersAndBlock(Stage stage, Set<Integer> euInvolved) {
		currentStageTracker = new StageTracker(stage.getStageId(), euInvolved);
		currentStageTracker.waitForStageToFinish();
	}
	
	public void waitForFinishedStageAndCompleteBookeeping(Stage stage) {
		currentStageTracker.waitForStageToFinish();
		// Check status of the stage
		if(currentStageTracker.finishedSuccessfully()) {
			Map<Integer, Set<DataReference>> results = currentStageTracker.getStageResults();
			setFinished(stage, results);
		}
		else{
			LOG.warn("Not successful stage... CHECK, non-FT fully-fledged implementation yet :|");
			System.exit(-1);
		}
	}

	public void finishStage(int euId, 
			int stageId, 
			Map<Integer, Set<DataReference>> results, 
			List<RuntimeEvent> runtimeEvents,
			Set<DatasetMetadata> managedDatasets) {
		// Keep runtimeEvents of last executed Stage
		if(runtimeEvents.size() > 0) {
			this.runtimeEventsInLastStageExecution = true;
			// Store runtimeEvents on a per node basis (eu -> execution unit)
			this.lastStageRuntimeEvents.put(euId, runtimeEvents);
		}
		else {
			this.runtimeEventsInLastStageExecution = false;
		}
		
		// Update DatasetRegistry 
		clusterDatasetRegistry.updateDatasetsForNode(euId, managedDatasets, stageId);
		
		// Then notify the stageTracker that the stage was successful
		currentStageTracker.notifyOk(euId, stageId, results);
	}
	
}
