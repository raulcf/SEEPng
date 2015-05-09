package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Set;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ScheduleDeployCommand implements CommandType {

	private SeepLogicalQuery slq;
	private ScheduleDescription sd;
	private Set<EndPoint> endpoints;
	
	public ScheduleDeployCommand(){ }
	
	public ScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription sd, Set<EndPoint> endpoints) {
		this.slq = slq;
		this.sd = sd;
		this.endpoints = endpoints;
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
	
	public Set<EndPoint> getEndPoints() {
		return endpoints;
	}

}
