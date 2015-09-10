package uk.ac.imperial.lsds.seep.comm.protocol;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ScheduleDeployCommand implements CommandType {

	private SeepLogicalQuery slq;
	private ScheduleDescription sd;
	
	public ScheduleDeployCommand(){ }
	
	public ScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription sd) {
		this.slq = slq;
		this.sd = sd;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.SCHEDULE_TASKS.type();
	}
	
	public SeepLogicalQuery getQuery(){
		return slq;
	}
	
	public ScheduleDescription getSchedule(){
		return sd;
	}

}
