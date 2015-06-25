package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;

public class StageStatusCommand implements CommandType {

	private int stageId;
	private int euId;
	private Status status;
	private Map<Integer, Set<DataReference>> resultDataReference;
	
	public StageStatusCommand() {}
	
	public StageStatusCommand(int stageId, int euId, Status status, Map<Integer, Set<DataReference>> producedOutput) {
		this.stageId = stageId;
		this.euId = euId;
		this.status = status;
		this.resultDataReference = producedOutput;
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
	
	public enum Status {
		FAIL,
		OK,
	}

}