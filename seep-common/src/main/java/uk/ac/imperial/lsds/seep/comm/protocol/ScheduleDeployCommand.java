package uk.ac.imperial.lsds.seep.comm.protocol;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ScheduleDeployCommand implements CommandType {

	private SeepLogicalQuery slq;
	private ScheduleDescription sd;
	private int StageNotificationPort;
	

	public ScheduleDeployCommand(){ }
	
	public ScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription sd) {
		this.slq = slq;
		this.sd = sd;
		this.StageNotificationPort = -1;
	}
	
	public ScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription sd, int notificationPort) {
		this.slq = slq;
		this.sd = sd;
		this.StageNotificationPort = notificationPort;
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

	/**
	 * @return the stageNotificationPort
	 */
	public int getStageNotificationPort() {
		return StageNotificationPort;
	}

	/**
	 * @param stageNotificationPort the stageNotificationPort to set
	 */
	public void setStageNotificationPort(int stageNotificationPort) {
		StageNotificationPort = stageNotificationPort;
	}

}
