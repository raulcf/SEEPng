package uk.ac.imperial.lsds.seepmaster.query;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seepmaster.scheduler.SchedulerEngine;

import com.esotericsoftware.kryo.Kryo;

public class ScheduledQueryManager implements QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(ScheduledQueryManager.class);
	
	private MasterConfig mc;
	private static ScheduledQueryManager sqm;
	private SchedulerEngine se;
	private SeepLogicalQuery slq;
	private String pathToQueryJar;
	
	private InfrastructureManager inf;
	private Map<Integer, EndPoint> opToEndpointMapping;
	private Comm comm;
	private Kryo k;
	private LifecycleManager lifeManager;
	
	private ScheduledQueryManager(InfrastructureManager inf, Map<Integer, EndPoint> opToEndpointMapping, 
			Comm comm, LifecycleManager lifeManager, MasterConfig mc){
		this.inf = inf;
		this.opToEndpointMapping = opToEndpointMapping;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		this.mc = mc;
	}
	
	public static ScheduledQueryManager getInstance(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, 
			Comm comm, LifecycleManager lifeManager, MasterConfig mc){
		if(sqm == null){
			return new ScheduledQueryManager(inf, mapOpToEndPoint, comm, lifeManager, mc);
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
		ScheduleDescription sd = se.buildSchedulingPlanForQuery(slq);
		LOG.info("Schedule Description:");
		LOG.info(sd.toString());
		
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean loadQueryFromFile(String pathToJar, String definitionClass, String[] queryArgs) {
		throw new NotImplementedException("ScheduledQueryManager.loadQueryFromFile not implemented !!");
	}

	@Override
	public boolean deployQueryToNodes() {
		// TODO Send all necessary info to workers, still to figure out what's the minimum.
		// SET trackers
		return false;
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

}
