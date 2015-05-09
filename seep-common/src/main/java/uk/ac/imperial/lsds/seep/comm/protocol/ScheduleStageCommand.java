package uk.ac.imperial.lsds.seep.comm.protocol;

public class ScheduleStageCommand implements CommandType {

	private int stageId;
	
	public ScheduleStageCommand() {}
	
	public ScheduleStageCommand(int stageId) {
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
