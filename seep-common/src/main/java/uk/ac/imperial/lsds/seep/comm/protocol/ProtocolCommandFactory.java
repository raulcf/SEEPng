package uk.ac.imperial.lsds.seep.comm.protocol;

import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;

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

	public static MasterWorkerCommand buildQueryDeployCommand(PhysicalSeepQuery originalQuery) {
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
	
}
