package uk.ac.imperial.lsds.seep.comm.protocol;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand.Status;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class ProtocolCommandFactory {
	
	/** MasterWorker commands **/
	
	public static MasterWorkerCommand buildBootstrapCommand(String ip, int port, int dataPort, int controlPort){
		BootstrapCommand bc = new BootstrapCommand(ip, port, dataPort, controlPort);
		MasterWorkerCommand c = new MasterWorkerCommand(bc);
		return c;
	}
	
	public static MasterWorkerCommand buildCodeCommand(byte[] data, String baseClassName, String[] queryConfig, String methodName){
		CodeCommand cc = new CodeCommand(data, baseClassName, queryConfig, methodName);
		MasterWorkerCommand c = new MasterWorkerCommand(cc);
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
	
	public static MasterWorkerCommand buildScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription scheduleDescription, int notificationPort) {
		ScheduleDeployCommand sdc = new ScheduleDeployCommand(slq, scheduleDescription, notificationPort);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		return c;
	}

	public static MasterWorkerCommand buildScheduleDeployCommand(SeepLogicalQuery slq, ScheduleDescription scheduleDescription) {
		ScheduleDeployCommand sdc = new ScheduleDeployCommand(slq, scheduleDescription);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		return c;
	}

	public static MasterWorkerCommand buildScheduleStageCommand(int stageId, Map<Integer, Set<DataReference>> input, Map<Integer, Set<DataReference>> ouptut) {
		ScheduleStageCommand sdc = new ScheduleStageCommand(stageId, input, ouptut);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		return c;
	}
	
	public static MasterWorkerCommand buildMaterializeTaskCommand(
			Map<Integer, EndPoint> opToEndpointMapping, 
			Map<Integer, Map<Integer, Set<DataReference>>> inputs, 
			Map<Integer, Map<Integer, Set<DataReference>>> outputs) {
		MaterializeTaskCommand mtc = new MaterializeTaskCommand(opToEndpointMapping, inputs, outputs);
		MasterWorkerCommand c = new MasterWorkerCommand(mtc);
		return c;
	}

	public static MasterWorkerCommand buildStageStatusCommand(int stageId, int euId, Status status, Map<Integer, Set<DataReference>> producedOutput) {
		StageStatusCommand ssc = new StageStatusCommand(stageId, euId, status, producedOutput);
		MasterWorkerCommand c = new MasterWorkerCommand(ssc);
		return c;
	}
	
	public static MasterWorkerCommand buildLocalSchedulerElectCommand(Set<EndPoint> endpoints){
		LocalSchedulerElectCommand lsec = new LocalSchedulerElectCommand(endpoints);
		MasterWorkerCommand c = new MasterWorkerCommand(lsec);
		return c;
	}
	
	public static MasterWorkerCommand buildLocalSchedulerStageCommand(int stageId,  Set<Stage> stages){
		LocalSchedulerStagesCommand lssc = new LocalSchedulerStagesCommand(stageId, stages);
		MasterWorkerCommand c = new MasterWorkerCommand(lssc);
		return c;
	}
	
	/** WorkerWorker commands 
	 * @param receivingDataPort 
	 * @param dataReferenceId 
	 * @param myIp **/
	
	public static WorkerWorkerCommand buildRequestDataReference(int dataReferenceId, InetAddress myIp, int receivingDataPort) {
		RequestDataReferenceCommand rdrc = new RequestDataReferenceCommand(dataReferenceId, myIp.getHostAddress(), receivingDataPort);
		WorkerWorkerCommand c = new WorkerWorkerCommand(rdrc);
		return c;
	}
	
}
