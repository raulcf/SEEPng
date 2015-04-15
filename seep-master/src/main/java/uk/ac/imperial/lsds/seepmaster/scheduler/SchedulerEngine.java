package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import uk.ac.imperial.lsds.seep.api.SeepLogicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.DistributedMutableState;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageStatus;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

public class SchedulerEngine {

	// FIXME: refactor this inside scheduledQueryManager
	
	final private Logger LOG = LoggerFactory.getLogger(SchedulerEngine.class);
	
	private static SchedulerEngine instance;
	private ScheduleTracker tracker;
	private SchedulingStrategy schedulingStrategy;
	private SeepLogicalQuery slq;
	private Thread worker;
	private SchedulerEngineWorker seWorker;
	
	private int stageId = 0;
	private Set<Stage> stages;
	
	private SchedulerEngine(MasterConfig mc) {
		schedulingStrategy = SchedulingStrategyType.clazz(mc.getInt(MasterConfig.SCHED_STRATEGY));
		stages = new HashSet<>();
	}
	
	public ScheduleTracker ___tracker_for_test() {
		return tracker;
	}
	
	public Stage __get_next_stage_to_schedule_fot_test() {
		return this.schedulingStrategy.next(tracker);
	}
	
	public static SchedulerEngine getInstance(MasterConfig mc) {
		if(instance == null){
			instance = new SchedulerEngine(mc);
		}
		return instance;
	}
	
	public ScheduleDescription buildSchedulingPlanForQuery(SeepLogicalQuery slq) {
		Set<Integer> opsAlreadyInSchedule = new HashSet<>();
		// Start building from sink
		SeepLogicalOperator op = (SeepLogicalOperator) slq.getSink();
		// Recursive method, with opsAlreadyInSchedule to detect already incorporated stages
		buildScheduleFromStage(null, op,  opsAlreadyInSchedule, slq);
		ScheduleDescription sd = new ScheduleDescription(stages);
		return sd;
	}
	
	public boolean initializeSchedulerEngine(InfrastructureManager inf, Comm comm, Kryo k) {
		// TODO: error handling, check stages is not null -> this was called appropriately
		
		// Initialize all tracking machinery
		tracker = new ScheduleTracker(stages);
		// Initialize the threads that will be broadcasting stuff to the machines
		seWorker = new SchedulerEngineWorker(schedulingStrategy, tracker, inf, comm, k);
		worker = new Thread(seWorker);
		
		return true;
	}
	
	public boolean prepareForStart(Set<Connection> connections) {
		// Set initial connections in worker
		seWorker.setConnections(connections);
		// Basically change stage status so that SOURCE tasks are ready to run
		boolean success = true;
		for(Stage stage : stages) {
			if(stage.getStageType().equals(StageType.UNIQUE_STAGE) || stage.getStageType().equals(StageType.SOURCE_STAGE)) {
				boolean changed = tracker.setReady(stage);
				success = success && changed;
			}
		}
		return success;
	}
	
	public boolean startScheduling() {
		worker.start();
		// FIXME: check for this condition
		return true;
	}
	
	public boolean stopScheduling() {
		try {
			worker.join();
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// FIXME: check for this condition
		return true;
	}
	
	public Set<Stage> returnReadyStages() {
		Set<Stage> toReturn = new HashSet<>();
		for(Stage stage : stages) {
			if(tracker.isStageReady(stage)) {
				toReturn.add(stage);
			}
		}
		return toReturn;
	}
	
	public void resetSchedule() {
		tracker.resetAllStagesTo(StageStatus.WAITING);
	}
	
	private Stage stageResponsibleFor(int opId) {
		for(Stage s : stages) {
			if(s.responsibleFor(opId)) {
				return s;
			}
		}
		return null;
	}
	
	private void buildScheduleFromStage(Stage parent, SeepLogicalOperator slo,  
			Set<Integer> opsAlreadyInSchedule, SeepLogicalQuery slq) {
		// Check whether this op has already been incorporated to a stage and abort if so
		int opId = slo.getOperatorId();
		if(opsAlreadyInSchedule.contains(opId)){
			// Create dependency with the stage governing opId in this case and return
			parent.dependsOn(stageResponsibleFor(opId));
			return;
		}
		// Create new stage and dependency with parent
		Stage stage = new Stage(stageId);
		if(parent != null){
			parent.dependsOn(stage);
		}
		stage = createStageFromLogicalOperator(stage, opsAlreadyInSchedule, slo);
		stages.add(stage);
		StageType type = stage.getStageType();

		// If we hit a source or unique stage, then just finish
		if(type.equals(StageType.SOURCE_STAGE) || type.equals(StageType.UNIQUE_STAGE)) {
			return;
		}
		
		// Update slo after stage creation
		slo = (SeepLogicalOperator) slq.getOperatorWithId(stage.getIdOfOperatorBoundingStage());
		
		// If multiple input explore for each
		if(stage.hasMultipleInput()){
			for(UpstreamConnection uc : slo.upstreamConnections()){
				SeepLogicalOperator upstreamOp = (SeepLogicalOperator) uc.getUpstreamOperator();
				stageId++;
				buildScheduleFromStage(stage, upstreamOp, opsAlreadyInSchedule, slq);
			}
		// If not explore the previous op
		} 
		else {
			SeepLogicalOperator upstreamOp = (SeepLogicalOperator)slo.upstreamConnections().get(0).getUpstreamOperator();
			stageId++;
			buildScheduleFromStage(stage, upstreamOp,  opsAlreadyInSchedule, slq);
		}
			
	}
	
	private Stage createStageFromLogicalOperator(Stage stage, Set<Integer> opsAlreadyInSchedule, SeepLogicalOperator slo){
		StageType type = null;
		boolean containsSinkOperator = false;
		boolean containsSourceOperator = false;
		
		boolean finishesStage = false;
		do {
			// get opId of current op
			int opId = slo.getOperatorId();
			// Add opId to stage
			stage.add(opId);
			opsAlreadyInSchedule.add(opId);
			if (isSink(slo)) containsSinkOperator = true;
			if (isSource(slo)) containsSourceOperator = true;
			// Check if it terminates stage
			// has partitioned state?
			if(slo.isStateful()) {
				if(slo.getState().getDMS().equals(DistributedMutableState.PARTITIONED)) {
					stage.setHasPartitionedState();
					finishesStage = true;
				}
			}
			
			// has multiple inputs?
			if(slo.upstreamConnections().size() > 1) {
				stage.setRequiresMultipleInput();
				finishesStage = true;
			}
			
			// is source operator?
			if(containsSourceOperator) {
				finishesStage = true;
			}
			// if not source op, then...
			else {
				// has upstream downstreams other than me?
				if(slo.upstreamConnections().get(0).getUpstreamOperator().downstreamConnections().size() > 1){	
					finishesStage = true;
				}
			}
			
			// Get next operator
			if(!finishesStage){
				slo = (SeepLogicalOperator)slo.upstreamConnections().get(0).getUpstreamOperator();
			}
			
		} while(!finishesStage);
		
		// Set stage type
		if(containsSourceOperator && containsSinkOperator){
			type = StageType.UNIQUE_STAGE;
		} else if(containsSourceOperator){
			type = StageType.SOURCE_STAGE;
		} else if(containsSinkOperator){
			type = StageType.SINK_STAGE;
		} else {
			type = StageType.INTERMEDIATE_STAGE;
		}
		stage.setStageType(type);
		return stage;
	}
	
	private boolean isSink(SeepLogicalOperator slo){
		if(slo.getSeepTask() instanceof Sink) {
			return true;
		}
		return false;
	}
	
	private boolean isSource(SeepLogicalOperator slo){
		if(slo.getSeepTask() instanceof Source) {
			return true;
		}
		return false;
	}

	public void finishStage(int euId, int stageId) {
		tracker.finishStage(euId, stageId);
	}
	
}
