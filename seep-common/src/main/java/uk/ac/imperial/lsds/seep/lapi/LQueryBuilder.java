package uk.ac.imperial.lsds.seep.lapi;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.SeepState;

public class LQueryBuilder implements LAPI {
	
	/**
	 * LAPI implementation
	 */
	
	
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
	public Operator newStatefulSource(Source seepTask, SeepState state, int opId) {
		return queryAPI.newStatefulSource(seepTask, state, opId);
	}

	@Override
	public Operator newStatelessSource(Source seepTask, int opId) {
		return queryAPI.newStatelessSource(seepTask, opId);
	}

	@Override
	public Operator newStatefulOperator(SeepTask seepTask, SeepState state, int opId) {
		return queryAPI.newStatefulOperator(seepTask, state, opId);
	}

	@Override
	public Operator newStatelessOperator(SeepTask seepTask, int opId) {
		return queryAPI.newStatelessOperator(seepTask, opId);
	}

	@Override
	public Operator newStatefulSink(Sink seepTask, SeepState state, int opId) {
		return queryAPI.newStatefulSink(seepTask, state, opId);
	}

	@Override
	public Operator newStatelessSink(Sink seepTask, int opId) {
		return queryAPI.newStatelessSink(seepTask, opId);
	}

	@Override
	public void setInitialPhysicalInstancesForLogicalOperator(int opId, int numInstances) {
		System.out.println("TODO: ERROR, invalid option");
		System.exit(0);
	}

}
