package uk.ac.imperial.lsds.seep.api;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;

public class ScheduleBuilder implements ScheduleAPI {

	final private Logger LOG = LoggerFactory.getLogger(ScheduleBuilder.class);
	
	private Set<Stage> stages = new HashSet<>();
	
	/**
	 * ScheduleAPI implementation
	 */
	
	public ScheduleDescription build() {
		// TODO: make sure stages were declared, defined, etc
		ScheduleDescription sd = new ScheduleDescription(stages, queryAPI.getQueryOperators());
		return sd;
	}
	
	@Override
	public boolean declareStages(Stage... stagesToDeclare) {
		for(Stage s : stagesToDeclare) {
			stages.add(s);
		}
		return true;
	}
	
	@Override
	public Stage createStage(int stageId, int opId, StageType t, ControlEndPoint location) {
		// TODO: do all error checking here
		if(t.equals(StageType.SINK_STAGE) && stageId != 0) {
			LOG.error("Sink stage must have id == 0 (fix this constrain)");
			System.exit(0);
		}
		Stage stage = new Stage(stageId);
		stage.add(opId);
		stage.setStageType(t);
		stage.setStageLocation(location);
		return stage;
	}
	
	
	/**
	 * QueryAPI implementation
	 */
	
	private static QueryBuilder queryAPI = new QueryBuilder();

	@Override
	public List<LogicalOperator> getQueryOperators() {
		return queryAPI.getQueryOperators();
	}

	@Override
	public List<SeepState> getQueryState() {
		return queryAPI.getQueryState();
	}

	@Override
	public int getInitialPhysicalInstancesPerLogicalOperator(int logicalOperatorId) {
		return queryAPI.getInitialPhysicalInstancesPerLogicalOperator(logicalOperatorId);
	}

	@Override
	public List<LogicalOperator> getSources() {
		return queryAPI.getSources();
	}

	@Override
	public LogicalOperator getSink() {
		return queryAPI.getSink();
	}

	@Override
	public LogicalOperator newStatefulSource(Source seepTask, SeepState state, int opId) {
		return queryAPI.newStatefulSource(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessSource(Source seepTask, int opId) {
		return queryAPI.newStatelessSource(seepTask, opId);
	}

	@Override
	public LogicalOperator newStatefulOperator(SeepTask seepTask, SeepState state, int opId) {
		return queryAPI.newStatefulOperator(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessOperator(SeepTask seepTask, int opId) {
		return queryAPI.newStatelessOperator(seepTask, opId);
	}

	@Override
	public LogicalOperator newStatefulSink(Sink seepTask, SeepState state, int opId) {
		return queryAPI.newStatefulSink(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessSink(Sink seepTask, int opId) {
		return queryAPI.newStatelessSink(seepTask, opId);
	}
	
	@Override
	public Operator newChooseOperator(SeepChooseTask choose, int opId) {
		return queryAPI.newChooseOperator(choose, opId);
	}

	@Override
	public void setInitialPhysicalInstancesForLogicalOperator(int opId, int numInstances) {
		System.out.println("TODO: ERROR, invalid option");
		System.exit(0);
	}
	
}
