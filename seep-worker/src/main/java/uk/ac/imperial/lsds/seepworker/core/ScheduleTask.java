package uk.ac.imperial.lsds.seepworker.core;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepworker.comm.WorkerMasterAPIImplementation;

public class ScheduleTask {

	private int stageId;
	private int euId;
	private Deque<LogicalOperator> operators;
	private SchedulePipelineCollector api;
	
	private WorkerMasterAPIImplementation masterApi;
	private Connection masterConn;
	
	private ScheduleTask(int euId, int stageId, Deque<LogicalOperator> operators, WorkerMasterAPIImplementation masterApi, Connection masterConn) {
		this.stageId = stageId;
		this.euId = euId;
		this.operators = operators;
		this.masterApi = masterApi;
		this.masterConn = masterConn;
	}
	
	public static ScheduleTask buildTaskFor(int id, Stage s, SeepLogicalQuery slq, WorkerMasterAPIImplementation masterApi, Connection masterConn) {
		Deque<Integer> wrappedOps = s.getWrappedOperators();
		Deque<LogicalOperator> operators = new ArrayDeque<>();
		while(! wrappedOps.isEmpty()) {
			LogicalOperator lo = slq.getOperatorWithId(wrappedOps.poll());
			operators.addLast(lo);
		}
		return new ScheduleTask(id, s.getStageId(), operators, masterApi, masterConn);
	}
	
	public void configureScheduleTaskLazily(List<OutputAdapter> outputAdapters) {
		this.api = new SchedulePipelineCollector(operators, outputAdapters);
	}
	
	public void triggerProcessingPipeline(ITuple iTuple) {
		api.processData(iTuple);
		api.rewindPipeline();
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public int getEuId() {
		return euId;
	}

	public void notifyStatusOk(Map<Integer, Set<DataReference>> producedOutput) {
		masterApi.scheduleTaskStatus(masterConn, stageId, euId, StageStatusCommand.Status.OK, producedOutput);
	}

}
