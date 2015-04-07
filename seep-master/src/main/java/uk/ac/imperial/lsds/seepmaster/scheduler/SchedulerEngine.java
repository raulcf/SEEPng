package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.SeepLogicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.DistributedMutableState;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;

public class SchedulerEngine {

	private static SchedulerEngine instance;
	private ScheduleTracker tracker;
	private SchedulingStrategy schedulingStrategy;
	private SeepLogicalQuery slq;
	
	private int stageId = 0;
	private Set<Stage> stages;
	
	private SchedulerEngine(MasterConfig mc) {
		schedulingStrategy = SchedulingStrategyType.clazz(mc.getInt(MasterConfig.SCHED_STRATEGY));
		stages = new HashSet<>();
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
		// Initialize all tracking machinery
		tracker = new ScheduleTracker(stages);
		return sd;
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
	
}
