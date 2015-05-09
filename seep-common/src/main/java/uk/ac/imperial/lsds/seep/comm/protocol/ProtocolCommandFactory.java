package uk.ac.imperial.lsds.seep.comm.protocol;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.SeepPhysicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ProtocolCommandFactory {
	
	public static MasterWorkerCommand buildBootstrapCommand(String ip, int port, int dataPort){
		BootstrapCommand bc = new BootstrapCommand(ip, port, dataPort);
		MasterWorkerCommand c = new MasterWorkerCommand(bc);
		return c;
	}
	
	public static MasterWorkerCommand buildCodeCommand(byte[] data){
		CodeCommand cc = new CodeCommand(data);
		MasterWorkerCommand c = new MasterWorkerCommand(cc);
		return c;
	}

	public static MasterWorkerCommand buildQueryDeployCommand(SeepPhysicalQuery originalQuery) {
		QueryDeployCommand qdc = new QueryDeployCommand(originalQuery);
		MasterWorkerCommand c = new MasterWorkerCommand(qdc);
		return c;
	}
	
	public static MasterWorkerCommand buildStartQueryCommand(){
		StartQueryCommand sqc = new StartQueryCommand();
		MasterWorkerCommand c = new MasterWorkerCommand(sqc);
		return c;
	}
	
	public static MasterWorkerCommand buildStopQueryCommand(){
		StopQueryCommand sqc = new StopQueryCommand();
		MasterWorkerCommand c = new MasterWorkerCommand(sqc);
		return c;
	}
	
	public static MasterWorkerCommand buildDeadWorkerCommand(int workerId, String reason){
		DeadWorkerCommand dwc = new DeadWorkerCommand(workerId, reason);
		MasterWorkerCommand c = new MasterWorkerCommand(dwc);
		return c;
	}

	public static MasterWorkerCommand buildScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription scheduleDescription) {
		ScheduleDeployCommand sdc = new ScheduleDeployCommand(slq, scheduleDescription);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		return c;
	}

	public static MasterWorkerCommand buildExecuteStageCommand(int stageId) {
		ScheduleStageCommand sdc = new ScheduleStageCommand(stageId);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		return c;
	}
	
}
