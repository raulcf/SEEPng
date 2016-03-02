package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;

public class StageStatusCommand implements CommandType {

	private int stageId;
	private int euId;
	private Status status;
	private Map<Integer, Set<DataReference>> resultDataReference;
	private List<RuntimeEvent> runtimeEvents;
	
	public StageStatusCommand() {}
	
	public StageStatusCommand(int stageId, int euId, Status status, Map<Integer, Set<DataReference>> producedOutput, List<RuntimeEvent> runtimeEvents) {
		this.stageId = stageId;
		this.euId = euId;
		this.status = status;
		this.resultDataReference = producedOutput;
		this.runtimeEvents = runtimeEvents;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.STAGE_STATUS.type();
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public int getEuId() {
		return euId;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public Map<Integer, Set<DataReference>> getResultDataReference() {
		return resultDataReference;
	}
	
	public List<RuntimeEvent> getRuntimeEvents() {
		return runtimeEvents;
	}
	
	public enum Status {
		FAIL,
		OK,
	}

}
