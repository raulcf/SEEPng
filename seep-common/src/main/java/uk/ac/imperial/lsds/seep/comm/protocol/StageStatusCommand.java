package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;

public class StageStatusCommand implements CommandType {

	private int stageId;
	private int euId;
	private Status status;
	private Set<DataReference> resultDataReference;
	
	public StageStatusCommand() {}
	
	public StageStatusCommand(int stageId, int euId, Status status, Set<DataReference> resultDataReference) {
		this.stageId = stageId;
		this.euId = euId;
		this.status = status;
		this.resultDataReference = resultDataReference;
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
	
	public Set<DataReference> getResultDataReference() {
		return resultDataReference;
	}
	
	public enum Status {
		FAIL,
		OK,
	}

}
