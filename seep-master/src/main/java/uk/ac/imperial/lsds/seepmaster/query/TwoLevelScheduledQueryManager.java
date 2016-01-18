package uk.ac.imperial.lsds.seepmaster.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnitGroup;
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
	private ArrayList<ExecutionUnitGroup> workerGroups;
	// Group of tasks ? possibly
	
	
	public TwoLevelScheduledQueryManager(InfrastructureManager inf, Comm comm, LifecycleManager lifeManager, MasterConfig mc){
		LOG.info("Initialising TwoLevelScheduled QueryManager");
		this.mc = mc;
		this.inf = inf;
		this.comm = comm;
		this.lifecycleManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		this.workerGroups = new ArrayList<ExecutionUnitGroup>();
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
		
		// TODO: Initialize the schedulerThread
//		seWorker = new SchedulerEngineWorker(
//				scheduleDescription, 
//				SchedulingStrategyType.clazz(mc.getInt(MasterConfig.SCHED_STRATEGY)), 
//				inf, 
//				comm, 
//				k);
//		scheduledEngineWorkerThread = new Thread(seWorker);
		LOG.info("Schedule Description:");
//		LOG.info(scheduleDescription.toString());
		
		this.lifecycleManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
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
		if (! (inf.executionUnitsAvailable() > 1)) {
			LOG.warn("Cannot deploy query, not enough nodes. Available: {} - Need at least 2!",
					inf.executionUnitsAvailable());
			return false;
		}
		
		createExecutionUnitGroups();
		//Elect Local Scheduler Node per Group and Send Command
		for (ExecutionUnitGroup eug : this.workerGroups) {
			eug.localSchedulerElect();
			Set<Integer> involvedEUId = new HashSet<>();
			involvedEUId.add(eug.getLocal_scheduler().getId());
			Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
			LOG.info("Sending Local Scheduler Elect to {} nodes", involvedEUId);
			MasterWorkerCommand scheduleElect = ProtocolCommandFactory.buildLocalSchedulerElectCommand(eug.getWorkerEndpoints());
			comm.send_object_sync(scheduleElect, connections, k);
			LOG.info("Sending Elect command...DONE!");
			
		}
		
		//Connect Local Schedulers with its workers 
		
		//Prepare scheduling engine - Have to define Group-Tasks
		
		// For now just forward all stages to local scheduler
		
		
		
		return false;
	}
	
	// Could be moved to a separate class - Called just once every time
	public void createExecutionUnitGroups() {
		int totalEUAvailable = inf.executionUnitsAvailable();
		for (int i = 0; i < totalEUAvailable; i++) {
			ExecutionUnit eu = inf.getExecutionUnit();
			boolean addedToGroup = false;
			// always create a new Group the first time
			if (i == 0) {
				ExecutionUnitGroup g = new ExecutionUnitGroup(eu.getEndPoint().getIpString());
				g.addToExecutionGroup(eu);
				this.workerGroups.add(g);
			} else {
				for (ExecutionUnitGroup g: this.workerGroups) {
					LOG.debug("Worker {} with ID {} belogs to group {}",eu.getEndPoint().getIp(), eu.getId(), g.belognsToGroup(eu));
					if (g.belognsToGroup(eu)) {
						g.addToExecutionGroup(eu);
						addedToGroup = true;
						break;
					}
				}
				// Could not find a suitable group
				if(!addedToGroup){
					ExecutionUnitGroup tmp = new ExecutionUnitGroup(eu.getEndPoint().getIpString());
					this.workerGroups.add(tmp);
				}
			}
		}
		
		LOG.debug("Current ExecutionUnit Groups: {}", this.workerGroups.size());
		for(ExecutionUnitGroup eug: this.workerGroups)
			LOG.debug(eug.toString());
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
