package uk.ac.imperial.lsds.seep.comm.protocol;

public class ExecuteStageCommand implements CommandType {

	private int stageId;
	
	public ExecuteStageCommand() {}
	
	public ExecuteStageCommand(int stageId) {
		this.stageId = stageId;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.SCHEDULE_STAGE.type();
	}
	
	public int getStageId() {
		return stageId;
	}

}
