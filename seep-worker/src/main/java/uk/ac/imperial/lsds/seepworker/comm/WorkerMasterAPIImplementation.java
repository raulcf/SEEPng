package uk.ac.imperial.lsds.seepworker.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.QueryDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StartQueryCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StopQueryCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;

import com.esotericsoftware.kryo.Kryo;

public class WorkerMasterAPIImplementation {

	final private Logger LOG = LoggerFactory.getLogger(WorkerMasterAPIImplementation.class.getName());
	
	private Conductor c;
	private Comm comm;
	private Kryo k;
	
	private String myIp;
	private int myPort;
	private int retriesToMaster;
	private int retryBackOffMs;
	
	public WorkerMasterAPIImplementation(Comm comm, Conductor c, WorkerConfig wc){
		this.comm = comm;
		this.c = c;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		this.myPort = wc.getInt(WorkerConfig.LISTENING_PORT);
		this.retriesToMaster = wc.getInt(WorkerConfig.MASTER_CONNECTION_RETRIES);
		this.retryBackOffMs = wc.getInt(WorkerConfig.MASTER_RETRY_BACKOFF_MS);
	}
	
	public void bootstrap(Connection masterConn, String myIp, int myPort, int dataPort){
		this.myIp = myIp;
		MasterWorkerCommand command = ProtocolCommandFactory.buildBootstrapCommand(myIp, myPort, dataPort);
		
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
	
	public void handleQueryDeploy(QueryDeployCommand qdc){
		InetAddress ip = null;
		try {
			ip = InetAddress.getByName(myIp);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		int myOwnId = Utils.computeIdFromIpAndPort(ip, myPort);
		PhysicalSeepQuery query = qdc.getQuery();
		
		// We don't know yet what is this for anyway...
		Set<EndPoint> meshTopology = query.getMeshTopology(myOwnId);
		
		PhysicalOperator po = query.getOperatorLivingInExecutionUnitId(myOwnId);
		LOG.info("Found PhysicalOperator: {} to execute in this executionUnit: {} stateful: {}", po.getOperatorName(), myOwnId, po.isStateful());
		c.deployPhysicalOperator(po, query);
	}

	public void handleStartQuery(StartQueryCommand sqc) {
		c.startProcessing();
	}
	
	public void handleStopQuery(StopQueryCommand sqc) {
		c.stopProcessing();
	}
	
}
