package uk.ac.imperial.lsds.seepmaster.query;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.DistributedMutableState;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seep.scheduler.engine.SchedulingStrategyType;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnitGroup;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.GlobalSchedulerEngineWorker;
import uk.ac.imperial.lsds.seepmaster.scheduler.ScheduleManager;

public class GlobalScheduledQueryManager implements  QueryManager, ScheduleManager{
	
	final private Logger LOG = LoggerFactory.getLogger(GlobalScheduledQueryManager.class);
	
	private MasterConfig mc;
	private static GlobalScheduledQueryManager tlsqm;
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
	private Thread globalscheduledEngineWorkerThread;
	private GlobalSchedulerEngineWorker seWorker;
	
	// Local Scheduler variables 
	private Set<ExecutionUnitGroup> workerGroups;
	Set<Integer> allInvolvedEUIds;
	// Group of tasks ? possibly
	
	
	public GlobalScheduledQueryManager(InfrastructureManager inf, Comm comm, LifecycleManager lifeManager, MasterConfig mc){
		LOG.info("Initialising TwoLevelScheduled QueryManager");
		this.mc = mc;
		this.inf = inf;
		this.comm = comm;
		this.lifecycleManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		allInvolvedEUIds = new HashSet<>();
	}
	
	// Static Singleton Access
	public static GlobalScheduledQueryManager getInstance(InfrastructureManager inf, Comm comm, 
			LifecycleManager lifeManager, MasterConfig mc){
		if(tlsqm == null)
			return new GlobalScheduledQueryManager(inf, comm, lifeManager, mc);
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
		// TODO: Refactor => Some duplicate methods with plain ScheduledQueryManager 
		this.scheduleDescription = this.buildSchedulingPlanForQuery(slq);
		
		// Initialize the Global SchedulerThread
		seWorker = new GlobalSchedulerEngineWorker(this.scheduleDescription,
				SchedulingStrategyType.clazz(mc.getInt(MasterConfig.SCHED_STRATEGY)), this.comm, this.k);
		this.globalscheduledEngineWorkerThread = new Thread(seWorker);
		LOG.info("Schedule Description:");
		LOG.info(scheduleDescription.toString());
		
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

		this.workerGroups = createExecutionUnitGroups();
		//Elect Local Scheduler Node per Group and Send Command
		LOG.info("Electing local schedulers");
		this.sendSchedulerElectCommand(workerGroups);
		LOG.info("Electing local schedulers...OK");
		
		//Send query to all nodes (could be a wise choice)
		Set<Connection> connections = this.getAllExecutionUnitsConnections();
		LOG.info("Sending query to {} nodes", connections.size());
		sendQueryToNodes(connections, definitionClassName, queryArgs, composeMethodName);
		LOG.info("Sending query to nodes...OK");
		
		// Send Schedule only to Local Schedulers
		connections = this.getLocalSchedulersConnections();
		LOG.info("Sending schedule to {} nodes", connections.size());
		sendScheduleToNodes(connections);
		LOG.info("Sending schedule to {} nodes...OK", connections.size() );
		
		// TODO: Prepare scheduling engine - define Group-Tasks
		LOG.info("Prepare scheduler engine...");
		seWorker.prepareForStart(connections, slq);
		LOG.info("Prepare scheduler engine...OK");
		
		// For now just forward all stages to local scheduler
		
		this.lifecycleManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		return true;
	}
	
	public void sendSchedulerElectCommand(Set<ExecutionUnitGroup> executionGroups){
		for (ExecutionUnitGroup eug : executionGroups) {
			eug.localSchedulerElect();
			Set<Integer> involvedEUId = new HashSet<>();
			involvedEUId.add(eug.getLocal_scheduler().getId());
			Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
			LOG.debug("Sending Local Scheduler Elect to {} nodes", involvedEUId);
			MasterWorkerCommand scheduleElect = ProtocolCommandFactory.buildLocalSchedulerElectCommand(eug.getWorkerEndpoints());
			comm.send_object_sync(scheduleElect, connections, k);
		}
	}
	
	public Set<Connection> getAllExecutionUnitsConnections(){
		return this.inf.getConnectionsTo(allInvolvedEUIds);
	}
	
	public Set<Connection> getLocalSchedulersConnections(){
		Set<Integer> involvedEUId = new HashSet<>();
		for(ExecutionUnitGroup eug : this.workerGroups){
			involvedEUId.add(eug.getLocal_scheduler().getId());
		}
		return this.inf.getConnectionsTo(involvedEUId);
	}
	
	// Could be moved to a separate class - Called just once every time
	public Set<ExecutionUnitGroup> createExecutionUnitGroups() {
		Set<ExecutionUnitGroup> toreturn = new HashSet<ExecutionUnitGroup>();
		int totalEUAvailable = inf.executionUnitsAvailable();
		for (int i = 0; i < totalEUAvailable; i++) {
			ExecutionUnit eu = inf.getExecutionUnit();
			this.allInvolvedEUIds.add(eu.getId());
			boolean addedToGroup = false;
			// always create a new Group the first time
			if (i == 0) {
				ExecutionUnitGroup g = new ExecutionUnitGroup(eu.getEndPoint().getIpString());
				g.addToExecutionGroup(eu);
				toreturn.add(g);
			} else {
				for (ExecutionUnitGroup g: toreturn) {
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
					toreturn.add(tmp);
				}
			}
		}
		
		LOG.debug("Current ExecutionUnit Groups: {}", toreturn.size());
		for(ExecutionUnitGroup eug: toreturn)
			LOG.debug(eug.toString());
		return toreturn;
	}
	
	public ScheduleDescription buildSchedulingPlanForQuery(SeepLogicalQuery slq) {
		Set<Integer> opsAlreadyInSchedule = new HashSet<>();
		// Start building from sink
		SeepLogicalOperator op = (SeepLogicalOperator) slq.getSink();
		// Recursive method, with opsAlreadyInSchedule to detect already incorporated stages
		Set<Stage> stages = new HashSet<>();
		int stageId = 0;
		buildScheduleFromStage(null, op,  opsAlreadyInSchedule, slq, stages, stageId);
		ScheduleDescription sd = new ScheduleDescription(stages);
		return sd;
	}
	
	private void buildScheduleFromStage(Stage parent, SeepLogicalOperator slo, Set<Integer> opsAlreadyInSchedule,
			SeepLogicalQuery slq, Set<Stage> stages, int stageId) {
		// Check whether this op has already been incorporated to a stage and
		// abort if so
		int opId = slo.getOperatorId();
		if (opsAlreadyInSchedule.contains(opId)) {
			// Create dependency with the stage governing opId in this case and
			// return
			Stage dependency = stageResponsibleFor(opId);
			parent.dependsOn(dependency);
			return;
		}
		// Create new stage and dependency with parent
		Stage stage = new Stage(stageId);
		if (parent != null) {
			parent.dependsOn(stage);
		}
		stage = createStageFromLogicalOperator(stage, opsAlreadyInSchedule, slo);
		stages.add(stage);
		StageType type = stage.getStageType();

		// If we hit a source or unique stage, then configure Input and finish
		if (type.equals(StageType.SOURCE_STAGE) || type.equals(StageType.UNIQUE_STAGE)) {
			return;
		}

		// Update slo after stage creation
		slo = (SeepLogicalOperator) slq.getOperatorWithId(stage.getIdOfOperatorBoundingStage());

		// If multiple input explore for each
		if (stage.hasMultipleInput()) {
			for (UpstreamConnection uc : slo.upstreamConnections()) {
				SeepLogicalOperator upstreamOp = (SeepLogicalOperator) uc.getUpstreamOperator();
				stageId++;
				buildScheduleFromStage(stage, upstreamOp, opsAlreadyInSchedule, slq, stages, stageId);
			}
			// If not explore the previous op
		} else {
			SeepLogicalOperator upstreamOp = (SeepLogicalOperator) slo.upstreamConnections().get(0)
					.getUpstreamOperator();
			stageId++;
			buildScheduleFromStage(stage, upstreamOp, opsAlreadyInSchedule, slq, stages, stageId);
		}
	}
	
	private Stage stageResponsibleFor(int opId) {
		for(Stage s : scheduleDescription.getStages()) {
			if(s.responsibleFor(opId)) {
				return s;
			}
		} 
		return null;
	}
	
	private Stage createStageFromLogicalOperator(Stage stage, Set<Integer> opsAlreadyInSchedule, SeepLogicalOperator slo) {
		StageType type = null;
		boolean containsSinkOperator = false;
		boolean containsSourceOperator = false;
		
		boolean finishesStage = false;
		do {
			// get opId of current op
			int opId = slo.getOperatorId();
			// Add opId to stage
			stage.add(opId);
			opsAlreadyInSchedule.add(opId);
			if (isSink(slo)) containsSinkOperator = true;
			if (isSource(slo)) containsSourceOperator = true;
			// Check if it terminates stage
			// has partitioned state?
			if(slo.isStateful()) {
				if(slo.getState().getDMS().equals(DistributedMutableState.PARTITIONED)) {
					stage.setHasPartitionedState();
					finishesStage = true;
				}
			}
			
			// has multiple inputs?
			if(slo.upstreamConnections().size() > 1) {
				stage.setRequiresMultipleInput();
				finishesStage = true;
			}
			
			// is source operator?
			if(containsSourceOperator) {
				finishesStage = true;
			}
			// if not source op, then...
			else {
				// has upstream downstreams other than me?
				Operator op = slo.upstreamConnections().get(0).getUpstreamOperator();
				if(op == null || op.downstreamConnections().size() > 1) {
					finishesStage = true;
				}
			}
			
			// Get next operator
			if(!finishesStage){
				slo = (SeepLogicalOperator)slo.upstreamConnections().get(0).getUpstreamOperator();
			}
			
		} while(!finishesStage);
		
		// Set stage type
		if(containsSourceOperator && containsSinkOperator) {
			type = StageType.UNIQUE_STAGE;
		} else if(containsSourceOperator) {
			type = StageType.SOURCE_STAGE;
		} else if(containsSinkOperator) {
			type = StageType.SINK_STAGE;
		} else {
			type = StageType.INTERMEDIATE_STAGE;
		}
		stage.setStageType(type);
		return stage;
	}
	
	private boolean isSink(SeepLogicalOperator slo) {
		if(slo.getSeepTask() instanceof Sink) {
			return true;
		}
		return false;
	}
	
	private boolean isSource(SeepLogicalOperator slo) {
		// Source if the op is a Source itself, or if its unique upstream is null. 
		// Null indicates that it's a tagging operator
		if(slo.getSeepTask() instanceof Source || slo.upstreamConnections().get(0).getUpstreamOperator() == null) {
			return true;
		}
		return false;
	}
	
	// FIXME: this code is repeated in materialisedQueryManager. please refactor
	// FIXME: in particular, consider moving this to
	// MasterWorkerAPIImplementation (that already handles a comm and k)
	private void sendQueryToNodes(Set<Connection> connections, String definitionClassName, String[] queryArgs, String composeMethodName) {
		// Send data file to nodes
		byte[] queryFile = Utils.readDataFromFile(pathToQueryJar);
		LOG.info("Sending query file of size: {} bytes", queryFile.length);
		MasterWorkerCommand code = ProtocolCommandFactory.buildCodeCommand(queryFile, definitionClassName, queryArgs, composeMethodName);
		comm.send_object_sync(code, connections, k);
		LOG.info("Sending query file...DONE!");
	}
	
	private boolean sendScheduleToNodes(Set<Connection> connections){
		LOG.info("Sending Schedule Deploy Command");
		// Send physical query to all nodes
		MasterWorkerCommand scheduleDeploy = ProtocolCommandFactory.buildScheduleDeployCommand(slq, scheduleDescription);
		boolean success = comm.send_object_sync(scheduleDeploy, connections, k);
		return success;
	}

	@Override
	public boolean startQuery() {
		LOG.info("Staring Scheduler");
		globalscheduledEngineWorkerThread.start();
		return true;
	}

	@Override
	public boolean stopQuery() {
		LOG.info("Stop scheduling");
		try {
			globalscheduledEngineWorkerThread.join();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	/** ScheduleManager interface Implementation **/
	
	@Override
	public void notifyStageStatus(StageStatusCommand ssc) {
		int stageId = ssc.getStageId();
		int euId = ssc.getEuId();
		Map<Integer, Set<DataReference>> results = ssc.getResultDataReference();
		StageStatusCommand.Status status = ssc.getStatus();
		seWorker.newStageStatus(stageId, euId, results, status);
	}
}
