package uk.ac.imperial.lsds.seep.comm.protocol;

public class StageStatusCommand implements CommandType {

	private int stageId;
	private int euId;
	private Status status;
	
	public StageStatusCommand() {}
	
	public StageStatusCommand(int stageId, int euId, Status status) {
		this.stageId = stageId;
		this.euId = euId;
		this.status = status;
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
	
	public enum Status {
		FAIL,
		OK,
	}

}
