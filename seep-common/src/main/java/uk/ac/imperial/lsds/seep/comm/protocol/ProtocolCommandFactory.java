package uk.ac.imperial.lsds.seep.comm.protocol;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand.Status;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

public class ProtocolCommandFactory {
	
	/** MasterWorker commands **/
	
	public static SeepCommand buildBootstrapCommand(String ip, int controlPort, int dataPort){
		BootstrapCommand bc = new BootstrapCommand(ip, controlPort, dataPort);
		MasterWorkerCommand c = new MasterWorkerCommand(bc);
		Command co = new Command(c);
		return co;
	}
	
	public static SeepCommand buildCodeCommand(short queryType, byte[] data, String baseClassName, String[] queryConfig, String methodName){
		CodeCommand cc = new CodeCommand(queryType, data, baseClassName, queryConfig, methodName);
		MasterWorkerCommand c = new MasterWorkerCommand(cc);
		Command co = new Command(c);
		return co;
	}
	
	public static SeepCommand buildStartQueryCommand(){
		StartQueryCommand sqc = new StartQueryCommand();
		MasterWorkerCommand c = new MasterWorkerCommand(sqc);
		Command co = new Command(c);
		return co;
	}
	
	public static SeepCommand buildStopQueryCommand(){
		StopQueryCommand sqc = new StopQueryCommand();
		MasterWorkerCommand c = new MasterWorkerCommand(sqc);
		Command co = new Command(c);
		return co;
	}
	
	public static SeepCommand buildDeadWorkerCommand(int workerId, String reason){
		DeadWorkerCommand dwc = new DeadWorkerCommand(workerId, reason);
		MasterWorkerCommand c = new MasterWorkerCommand(dwc);
		Command co = new Command(c);
		return co;
	}

	public static SeepCommand buildScheduleDeployCommand(ScheduleDescription scheduleDescription) {
		ScheduleDeployCommand sdc = new ScheduleDeployCommand(scheduleDescription);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		Command co = new Command(c);
		return co;
	}

	public static SeepCommand buildScheduleStageCommand(int stageId, Map<Integer, Set<DataReference>> input, Map<Integer, Set<DataReference>> ouptut) {
		ScheduleStageCommand sdc = new ScheduleStageCommand(stageId, input, ouptut);
		MasterWorkerCommand c = new MasterWorkerCommand(sdc);
		Command co = new Command(c);
		return co;
	}
	
	public static SeepCommand buildMaterializeTaskCommand(
			Map<Integer, ControlEndPoint> opToEndpointMapping, 
			Map<Integer, Map<Integer, Set<DataReference>>> inputs, 
			Map<Integer, Map<Integer, Set<DataReference>>> outputs) {
		MaterializeTaskCommand mtc = new MaterializeTaskCommand(opToEndpointMapping, inputs, outputs);
		MasterWorkerCommand c = new MasterWorkerCommand(mtc);
		Command co = new Command(c);
		return co;
	}

	public static SeepCommand buildStageStatusCommand(int stageId, int euId, Status status, Map<Integer, Set<DataReference>> producedOutput, List<RuntimeEvent> runtimeEvents) {
		StageStatusCommand ssc = new StageStatusCommand(stageId, euId, status, producedOutput, runtimeEvents);
		MasterWorkerCommand c = new MasterWorkerCommand(ssc);
		Command co = new Command(c);
		return co;
	}
	
	/** WorkerWorker commands 
	 * @param receivingDataPort 
	 * @param dataReferenceId 
	 * @param myIp **/
	
	public static SeepCommand buildRequestDataReference(int dataReferenceId, InetAddress myIp, int receivingDataPort) {
		RequestDataReferenceCommand rdrc = new RequestDataReferenceCommand(dataReferenceId, myIp.getHostAddress(), receivingDataPort);
		WorkerWorkerCommand c = new WorkerWorkerCommand(rdrc);
		Command co = new Command(c);
		return co;
	}
	
}
