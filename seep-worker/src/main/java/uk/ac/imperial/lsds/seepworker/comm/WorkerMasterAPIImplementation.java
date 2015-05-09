package uk.ac.imperial.lsds.seepworker.comm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.SeepPhysicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepPhysicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MaterializeTaskCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleStageCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StartQueryCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StopQueryCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
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
	
	private String pathToQueryJar;
	private String definitionClass;
	private String[] queryArgs;
	private String methodName;
	
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
	
	public void handleQueryInstantiation(String pathToQueryJar, String definitionClass, String[] queryArgs, String methodName) {
		this.pathToQueryJar = pathToQueryJar;
		this.definitionClass = definitionClass;
		this.queryArgs = queryArgs;
		this.methodName = methodName;
	}
	
	public void handleMaterializeTask(MaterializeTaskCommand mtc) {
		// Instantiate logical query
		SeepLogicalQuery slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, methodName);
		// Get physical info from command
		Map<Integer, EndPoint> mapping = mtc.getMapping();
		SeepPhysicalQuery query = makePhysicalQueryFrom(slq, mapping);
		int myOwnId = Utils.computeIdFromIpAndPort(getMyIp(), myPort);
		c.setQuery(myOwnId, query);
		c.materializeAndConfigureTask();
	}
	
	public void handleScheduleDeploy(ScheduleDeployCommand sdc) {
		// Instantiate logical query
		SeepLogicalQuery slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, methodName);
		// Get physical info from command
		Set<EndPoint> endpoints = sdc.getEndPoints();
		ScheduleDescription sd = sdc.getSchedule();
		// TODO:
		// TODO: in this case get all nodes involved in the schedule and then check shuffle phases
		SeepPhysicalQuery query = null;
		
		int myOwnId = Utils.computeIdFromIpAndPort(getMyIp(), myPort);
		c.setQuery(myOwnId, query);
		
	}

	public void handleStartQuery(StartQueryCommand sqc) {
		c.startProcessing();
	}
	
	public void handleStopQuery(StopQueryCommand sqc) {
		c.stopProcessing();
	}

	public void handleScheduleStage(ScheduleStageCommand esc) {
		// TODO Auto-generated method stub
		
	}
	
	private InetAddress getMyIp() {
		InetAddress ip = null;
		try {
			ip = InetAddress.getByName(myIp);
		} 
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ip;
	}
	
	private SeepPhysicalQuery makePhysicalQueryFrom(SeepLogicalQuery slq, Map<Integer, EndPoint> mapping) {
		Set<SeepPhysicalOperator> physicalOperators = new HashSet<>();
		
		for(Operator slo : slq.getAllOperators()) {
			int opId = slo.getOperatorId();
			EndPoint ep = mapping.get(opId);
			SeepPhysicalOperator po = SeepPhysicalOperator.createPhysicalOperatorFromLogicalOperatorAndEndPoint(slo, ep);
			physicalOperators.add(po);
		}
		
		SeepPhysicalQuery psq = SeepPhysicalQuery.buildPhysicalQueryFrom(physicalOperators, slq);
		return psq;
	}
	
}
