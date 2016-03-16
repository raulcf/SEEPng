package uk.ac.imperial.lsds.seep.api;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.SeepState;

public interface QueryAPI {
	
	public List<LogicalOperator> getQueryOperators();
	public List<SeepState> getQueryState();
	public int getInitialPhysicalInstancesPerLogicalOperator(int logicalOperatorId);
	public List<LogicalOperator> getSources();
	public LogicalOperator getSink();
	
	public Operator newStatefulSource(Source seepTask, SeepState state, int opId);
	public Operator newStatelessSource(Source seepTask, int opId);
	public Operator newStatefulOperator(SeepTask seepTask, SeepState state, int opId);
	public Operator newStatelessOperator(SeepTask seepTask, int opId);
	public Operator newStatefulSink(Sink seepTask, SeepState state, int opId);
	public Operator newStatelessSink(Sink seepTask, int opId);
	
	// FIXME: MDF API -- refactor outside
	public Operator newChooseOperator(SeepChooseTask choose, int opId);
	
	public void setInitialPhysicalInstancesForLogicalOperator(int opId, int numInstances);
	
	public String toString();
}
