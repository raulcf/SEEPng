package uk.ac.imperial.lsds.seepmaster.query;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.SchedulerEngine;

import com.esotericsoftware.kryo.Kryo;

public class ScheduledQueryManager implements QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(ScheduledQueryManager.class);
	
	private MasterConfig mc;
	private static ScheduledQueryManager sqm;
	private SchedulerEngine se;
	private ScheduleDescription scheduleDescription;
	private SeepLogicalQuery slq;
	private String pathToQueryJar;
	
	private InfrastructureManager inf;
	private Comm comm;
	private Kryo k;
	private LifecycleManager lifeManager;
	
	private ScheduledQueryManager(InfrastructureManager inf, Comm comm, LifecycleManager lifeManager, MasterConfig mc){
		this.inf = inf;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		this.mc = mc;
	}
	
	public static ScheduledQueryManager getInstance(InfrastructureManager inf, Comm comm, 
			LifecycleManager lifeManager, MasterConfig mc){
		if(sqm == null){
			return new ScheduledQueryManager(inf, comm, lifeManager, mc);
		}
		else{
			return sqm;
		}
	}
	
	@Override
	public boolean loadQueryFromParameter(SeepLogicalQuery slq, String pathToQueryJar) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.slq = slq;
		this.pathToQueryJar = pathToQueryJar;
		LOG.debug("Logical query loaded: {}", slq.toString());
		
		// Create Scheduler Engine and build scheduling plan for the given query
		se = SchedulerEngine.getInstance(mc);
		scheduleDescription = se.buildSchedulingPlanForQuery(slq);
		se.initializeSchedulerEngine();
		LOG.info("Schedule Description:");
		LOG.info(scheduleDescription.toString());
		
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean loadQueryFromFile(String pathToJar, String definitionClass, String[] queryArgs) {
		throw new NotImplementedException("ScheduledQueryManager.loadQueryFromFile not implemented !!");
	}

	@Override
	public boolean deployQueryToNodes() {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// Check that there is at least one resource available
		if(! (inf.executionUnitsAvailable() > 0)){
			LOG.warn("Cannot deploy query, not enough nodes. Available: {}", inf.executionUnitsAvailable());
			return false;
		}
		
		// TODO: figure out how to get this
		Set<Integer> involvedEUId = null;
		
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		LOG.info("Sending query and schedule to nodes");
		sendQueryCodeToNodes(connections);
		sendScheduleToNodes(connections);
		LOG.info("Seding query and schedule to nodes...OK {}");
		
		LOG.info("Prepare scheduler engine...");
		se.prepareForStart();
		LOG.info("Prepare scheduler engine...OK");
		
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		return true;
	}

	@Override
	public boolean startQuery() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean stopQuery() {
		// TODO Auto-generated method stub
		return false;
	}

	// FIXME: this code is repeated in materialisedQueryManager. please refactor
	// FIXME: in particular, consider moving this to MasterWorkerAPIImplementation (that already handles a comm and k)
	private boolean sendQueryCodeToNodes(Set<Connection> connections){
		byte[] queryFile = Utils.readDataFromFile(pathToQueryJar);
		LOG.info("Sending query file of size: {} bytes", queryFile.length);
		MasterWorkerCommand code = ProtocolCommandFactory.buildCodeCommand(queryFile);
		boolean success = comm.send_object_sync(code, connections, k);
		LOG.info("Sending query file...DONE!");
		return success;
	}
	
	private boolean sendScheduleToNodes(Set<Connection> connections){
		LOG.info("Sending Schedule Deploy Command");
		// Send physical query to all nodes
		MasterWorkerCommand scheduleDeploy = ProtocolCommandFactory.buildScheduleDeployCommand(slq, scheduleDescription);
		boolean success = comm.send_object_sync(scheduleDeploy, connections, k);
		return success;
	}
}
