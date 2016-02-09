package uk.ac.imperial.lsds.seep.comm.protocol;

import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ScheduleDeployCommand implements CommandType {

	private ScheduleDescription sd;
	
	public ScheduleDeployCommand(){ }
	
	public ScheduleDeployCommand(ScheduleDescription sd) {
		this.sd = sd;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.SCHEDULE_TASKS.type();
	}
	
	public ScheduleDescription getSchedule(){
		return sd;
	}

}
