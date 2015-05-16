package uk.ac.imperial.lsds.seepworker.core;

import java.util.ArrayDeque;
import java.util.Deque;

import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class ScheduleTask {

	private int stageId;
	private int euId;
	private PipelineCollector api;
	
	private ScheduleTask(int euId, int stageId, Deque<LogicalOperator> operators) {
		this.stageId = stageId;
		this.euId = euId;
		Deque<SeepTask> tasks = new ArrayDeque<>();
		for(LogicalOperator lo : operators) {
			tasks.add(lo.getSeepTask());
		}
		this.api = new PipelineCollector(tasks);
	}
	
	public static ScheduleTask buildTaskFor(int id, Stage s, SeepLogicalQuery slq) {
		Deque<Integer> wrappedOps = s.getWrappedOperators();
		Deque<LogicalOperator> operators = new ArrayDeque<>();
		while(! wrappedOps.isEmpty()) {
			LogicalOperator lo = slq.getOperatorWithId(wrappedOps.poll());
			operators.addLast(lo);
		}
		return new ScheduleTask(id, s.getStageId(), operators);
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public int getEuId() {
		return euId;
	}

}
