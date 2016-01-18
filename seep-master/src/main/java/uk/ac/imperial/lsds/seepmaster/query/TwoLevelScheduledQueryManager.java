package uk.ac.imperial.lsds.seepmaster.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.SchedulerEngineWorker;
import uk.ac.imperial.lsds.seepmaster.scheduler.SchedulingStrategyType;

public class TwoLevelScheduledQueryManager implements QueryManager{
	
	final private Logger LOG = LoggerFactory.getLogger(TwoLevelScheduledQueryManager.class);
	
	private MasterConfig mc;
	private static TwoLevelScheduledQueryManager tlsqm;
	private SeepLogicalQuery slq;
	
	private String pathToQueryJar;
	private String definitionClassName;
	private String[] queryArgs;
	private String composeMethodName;
	
	private InfrastructureManager inf;
	private Comm comm;
	private Kryo k;
	private LifecycleManager lifecycleManager;
	
	// Global Scheduler variables
	private ScheduleDescription scheduleDescription;
	private Thread scheduledEngineWorkerThread;
	private SchedulerEngineWorker seWorker;
	
	// Local Scheduler variables 
	
	
	public TwoLevelScheduledQueryManager(InfrastructureManager inf, Comm comm, LifecycleManager lifeManager, MasterConfig mc){
		this.mc = mc;
		this.inf = inf;
		this.comm = comm;
		this.lifecycleManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
	}
	
	// Static Singleton Access
	public static TwoLevelScheduledQueryManager getInstance(InfrastructureManager inf, Comm comm, 
			LifecycleManager lifeManager, MasterConfig mc){
		if(tlsqm == null)
			return new TwoLevelScheduledQueryManager(inf, comm, lifeManager, mc);
		else 
			return tlsqm;
	}
	
	/** QueryManager interface Implementation **/
	@Override
	public boolean loadQueryFromParameter(SeepLogicalQuery slq, String pathToQueryJar, String definitionClass,
			String[] queryArgs, String composeMethod) {
		boolean allowed = this.lifecycleManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.slq = slq;
		this.pathToQueryJar = pathToQueryJar;
		this.definitionClassName = definitionClass;
		this.queryArgs = queryArgs;
		this.composeMethodName = composeMethod;
		
		LOG.debug("Logical query loaded: {}", slq.toString());
		
		// Create Scheduler Engine and build scheduling plan for the given query
		//scheduleDescription = this.buildSchedulingPlanForQuery(slq);
		
		// Initialize the schedulerThread
		seWorker = new SchedulerEngineWorker(
				scheduleDescription, 
				SchedulingStrategyType.clazz(mc.getInt(MasterConfig.SCHED_STRATEGY)), 
				inf, 
				comm, 
				k);
		scheduledEngineWorkerThread = new Thread(seWorker);
		LOG.info("Schedule Description:");
		LOG.info(scheduleDescription.toString());
		
		return true;
	}

	@Override
	public boolean loadQueryFromFile(String pathToQueryJar, String definitionClass, String[] queryArgs, String composeMethod) {
		throw new NotImplementedException("TwoLevelScheduledQueryManager.loadQueryFromFile not implemented yet !!");
	}

	@Override
	public boolean deployQueryToNodes() {
		boolean allowed = this.lifecycleManager.canTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		if(!allowed) {
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// Check that there is at least one resource available
		// Need at least two nodes - One Local scheduler and a worker 
		if (!(inf.executionUnitsAvailable() > 1)) {
			LOG.warn("Cannot deploy query, not enough nodes. Available: {} - Need at least 2!", inf.executionUnitsAvailable());
			return false;
		}
		
		// Group nodes by IP
		
		
		//Select Local Schedulers per group
		
		
		//SEND elect command
		
		//Connect Schedule-worker with Local Schedulers
		
		//Prepare scheduling engine
		
		
		return false;
	}

	@Override
	public boolean startQuery() {
		LOG.info("Staring Scheduler");
		scheduledEngineWorkerThread.start();
		return true;
	}

	@Override
	public boolean stopQuery() {
		LOG.info("Stop scheduling");
		try {
			scheduledEngineWorkerThread.join();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	/** ScheduleManager interface Implementation **/

}
