package uk.ac.imperial.lsds.seep.comm.protocol;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand.Status;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ProtocolCommandFactory {
	
	/** MasterWorker commands **/
	
	public static MasterWorkerCommand buildBootstrapCommand(String ip, int port, int dataPort){
		BootstrapCommand bc = new BootstrapCommand(ip, port, dataPort);
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
