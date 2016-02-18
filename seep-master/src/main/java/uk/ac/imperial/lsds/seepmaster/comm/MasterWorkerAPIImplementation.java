package uk.ac.imperial.lsds.seepmaster.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.protocol.DeadWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.GenericQueryManager;
import uk.ac.imperial.lsds.seepmaster.query.ScheduledQueryManager;


public class MasterWorkerAPIImplementation {

	final private Logger LOG = LoggerFactory.getLogger(MasterWorkerAPIImplementation.class.getName());
	
	private GenericQueryManager qm;
	private InfrastructureManager inf;
	
	public MasterWorkerAPIImplementation(GenericQueryManager qm, InfrastructureManager inf) {
		this.qm = qm;
		this.inf = inf;
	}
	
	public void bootstrapCommand(uk.ac.imperial.lsds.seep.comm.protocol.BootstrapCommand bc){
		InetAddress bootIp = null;
		try {
			String ipStr = bc.getIp();
			bootIp = InetAddress.getByName(ipStr);
		}
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		int dataPort = bc.getDataPort();
		int controlPort = bc.getControlPort();
		int workerId = Utils.computeIdFromIpAndPort(bootIp, controlPort);
		LOG.info("New worker [id-> {}] node in {}:{}, dataPort: {}", workerId, bootIp.getHostAddress(), controlPort, dataPort);
		ExecutionUnit eu = inf.buildExecutionUnit(bootIp, controlPort, dataPort);
		inf.addExecutionUnit(eu);
	}
	
	public void handleDeadWorker(DeadWorkerCommand dwc){
		int workerId = dwc.getWorkerId();
		String reason = dwc.reason();
		LOG.warn("Worker {} has died, reason: {}", workerId, reason);
		inf.removeExecutionUnit(workerId);
	}
	
	public void stageStatusCommand(StageStatusCommand ssc) {
		((ScheduledQueryManager)qm.getQueryManager()).notifyStageStatus(ssc);
	}
	
}
