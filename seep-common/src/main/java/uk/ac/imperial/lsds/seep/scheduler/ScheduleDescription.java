package uk.ac.imperial.lsds.seep.scheduler;

import java.util.List;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.util.Utils;

public class ScheduleDescription {

	private Set<Stage> stages;
	private List<LogicalOperator> ops;
	
	public ScheduleDescription() { }
	
	/**
	 * Stages that form the schedule and list of operators wrapped up by the stages
	 * for convenience.
	 * @param stages
	 * @param ops
	 */
	public ScheduleDescription(Set<Stage> stages, List<LogicalOperator> ops) {
		this.stages = stages;
		this.ops = ops;
	}
	
	public Set<Stage> getStages() {
		return stages;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Schedule with "+stages.size()+" stages");
		sb.append(Utils.NL);
		return sb.toString();
	}

	public Stage getStageWithId(int stageId) {
		for(Stage s : stages) {
			if(s.getStageId() == stageId){
				return s;
			}
		}
		return null;
	}
	
	// Convenience methods. These are copied from other places in general.
	
	public LogicalOperator getOperatorWithId(int opId){
		for(LogicalOperator lo : ops){
			if(lo.getOperatorId() == opId)
				return lo;
		}
		return null;
	}
	
}
