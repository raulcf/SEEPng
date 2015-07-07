package uk.ac.imperial.lsds.seepworker.comm;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand.Status;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

import com.esotericsoftware.kryo.Kryo;

public class WorkerMasterAPIImplementation {

	final private Logger LOG = LoggerFactory.getLogger(WorkerMasterAPIImplementation.class.getName());
	
	private Comm comm;
	private Kryo k;
	
	private int retriesToMaster;
	private int retryBackOffMs;
	
	public WorkerMasterAPIImplementation(Comm comm, WorkerConfig wc){
		this.comm = comm;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		this.retriesToMaster = wc.getInt(WorkerConfig.MASTER_CONNECTION_RETRIES);
		this.retryBackOffMs = wc.getInt(WorkerConfig.MASTER_RETRY_BACKOFF_MS);
	}
	
	public void bootstrap(Connection masterConn, String myIp, int myPort, int dataPort, int controlPort){
		MasterWorkerCommand command = ProtocolCommandFactory.buildBootstrapCommand(myIp, myPort, dataPort, controlPort);
		LOG.info("Bootstrapping...");
		comm.send_object_async(command, masterConn, k, retriesToMaster, retryBackOffMs);
		LOG.info("Bootstrapping OK conn to master: {}", masterConn.toString());
	}
	
	public void signalDeadWorker(Connection masterConn, int workerId, String reason){
		MasterWorkerCommand command = ProtocolCommandFactory.buildDeadWorkerCommand(workerId, reason);
		LOG.info("Sending bye message to master...");
		// Retry to reconnect to master (master is highly available, will be alive eventually)
		comm.send_object_async(command, masterConn, k, retriesToMaster, retryBackOffMs);
		LOG.info("Sending bye message to master...OK");
	}
	
	public void scheduleTaskStatus(Connection masterConn, int stageId, int euId, Status status, Map<Integer, Set<DataReference>> producedOutput) {
		MasterWorkerCommand command = ProtocolCommandFactory.buildStageStatusCommand(stageId, euId, status, producedOutput);
		LOG.debug("Send stage {} status {} to master...", stageId, status.toString());
		comm.send_object_async(command, masterConn, k, retriesToMaster, retryBackOffMs);
	}
	
}
