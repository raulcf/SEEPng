package uk.ac.imperial.lsds.seepmaster.query;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.api.operator.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.sinks.MarkerSink;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.DistributedMutableState;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.SeepCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.LoadBalancingStrategyType;
import uk.ac.imperial.lsds.seepmaster.scheduler.ScheduleManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.ScheduleTracker;
import uk.ac.imperial.lsds.seepmaster.scheduler.SchedulerEngineWorker;
import uk.ac.imperial.lsds.seepmaster.scheduler.SchedulingStrategyType;

public class ScheduledQueryManager implements QueryManager, ScheduleManager {

	final private Logger LOG = LoggerFactory.getLogger(ScheduledQueryManager.class);
	
	private MasterConfig mc;
	private static ScheduledQueryManager sqm;
	
	private String pathToQueryJar;
	private String definitionClassName;
	private String[] queryArgs;
	private String composeMethodName;
	private short queryType;
	
	private InfrastructureManager inf;
	private Comm comm;
	private Kryo k;
	private LifecycleManager lifeManager;
	
	// Scheduler machinery
	private ScheduleDescription scheduleDescription;
	private Thread worker;
	private SchedulerEngineWorker seWorker;
	
	private ScheduledQueryManager(InfrastructureManager inf, Comm comm, LifecycleManager lifeManager, MasterConfig mc, short queryType){
		this.mc = mc;
		this.inf = inf;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
		this.queryType = queryType;
	}
	
	public static ScheduledQueryManager getInstance(InfrastructureManager inf, Comm comm, 
			LifecycleManager lifeManager, MasterConfig mc, short queryType){
		if(sqm == null){
			return new ScheduledQueryManager(inf, comm, lifeManager, mc, queryType);
		}
		else{
			return sqm;
		}
	}
	
	/** Implement QueryManager interface **/
	
	@Override
	public boolean loadQueryFromParameter(short queryType, SeepLogicalQuery slq, String pathToQueryJar, String definitionClass, 
			String[] queryArgs, String composeMethod) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.pathToQueryJar = pathToQueryJar;
		this.definitionClassName = definitionClass;
		this.queryArgs = queryArgs;
		this.composeMethodName = composeMethod;
		
		LOG.debug("Logical query loaded: {}", slq.toString());
		
		// Create Scheduler Engine and build scheduling plan for the given query
		scheduleDescription = this.buildSchedulingPlanForQuery(slq);
		
		loadSchedule(scheduleDescription, pathToQueryJar, definitionClassName, queryArgs, composeMethodName);
		return true;
	}
	
	public boolean loadSchedule(
			ScheduleDescription scheduleDescription, 
			String pathToQueryJar,
			String definitionClass,
			String[] queryArgs,
			String composeMethod) {
		// Create Scheduler Engine for the given schedule
		this.scheduleDescription = scheduleDescription;
		this.pathToQueryJar = pathToQueryJar;
		this.definitionClassName = definitionClass;
		this.queryArgs = queryArgs;
		this.composeMethodName = composeMethod;
		// Initialize the schedulerThread
		seWorker = new SchedulerEngineWorker(
				scheduleDescription, 
				SchedulingStrategyType.clazz(mc.getInt(MasterConfig.SCHED_STRATEGY)),
				LoadBalancingStrategyType.clazz(mc.getInt(MasterConfig.SCHED_STAGE_ASSIGMENT_STRATEGY)),
				inf, 
				comm, 
				k);
		worker = new Thread(seWorker);
		LOG.info("Schedule Description:");
		LOG.info(scheduleDescription.toString());
		
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean loadQueryFromFile(short queryType, String pathToQueryJar, String definitionClass, String[] queryArgs, String composeMethod) {
		throw new NotImplementedException("ScheduledQueryManager.loadQueryFromFile not implemented !!");
	}

	@Override
	public boolean deployQueryToNodes() {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		if(!allowed) {
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// Check that there is at least one resource available
		if(! (inf.executionUnitsAvailable() > 0)) {
			LOG.warn("Cannot deploy query, not enough nodes. Available: {}", inf.executionUnitsAvailable());
			return false;
		}
		
		// We want to be able to schedule tasks in any node in the cluster, so send to all
		Set<Integer> involvedEUId = new HashSet<>();
		int totalEUAvailable = inf.executionUnitsAvailable();
		for(int i = 0; i < totalEUAvailable; i++) {
			ExecutionUnit eu = inf.getExecutionUnit();
			involvedEUId.add(eu.getId());
		}
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		LOG.info("Sending query and schedule to nodes");
		sendQueryToNodes(queryType, connections, definitionClassName, queryArgs, composeMethodName);
		sendScheduleToNodes(connections);
		LOG.info("Sending query and schedule to nodes...OK {}");
		
		LOG.info("Prepare scheduler engine...");
		// Get the input info for the first stages
		seWorker.prepareForStart(connections);
		LOG.info("Prepare scheduler engine...OK");
		
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		return true;
	}

	@Override
	public boolean startQuery() {
		LOG.info("Staring Scheduler");
		worker.start();
		return true;
	}

	@Override
	public boolean stopQuery() {
		LOG.info("Stop scheduling");
		try {
			worker.join();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		return true;
	}

	// FIXME: this code is repeated in materialisedQueryManager. please refactor
	// FIXME: in particular, consider moving this to MasterWorkerAPIImplementation (that already handles a comm and k)
	private void sendQueryToNodes(short queryType, Set<Connection> connections, String definitionClassName, String[] queryArgs, String composeMethodName) {
		// Send data file to nodes
		byte[] queryFile = Utils.readDataFromFile(pathToQueryJar);
		LOG.info("Sending query file of size: {} bytes", queryFile.length);
		SeepCommand code = ProtocolCommandFactory.buildCodeCommand(queryType, queryFile, definitionClassName, queryArgs, composeMethodName);
		comm.send_object_sync(code, connections, k);
		LOG.info("Sending query file...DONE!");
	}
	
	private boolean sendScheduleToNodes(Set<Connection> connections){
		LOG.info("Sending Schedule Deploy Command");
		// Send physical query to all nodes
		SeepCommand scheduleDeploy = ProtocolCommandFactory.buildScheduleDeployCommand(scheduleDescription);
		boolean success = comm.send_object_sync(scheduleDeploy, connections, k);
		return success;
	}
	
	public ScheduleDescription buildSchedulingPlanForQuery(SeepLogicalQuery slq) {
		Set<Integer> opsAlreadyInSchedule = new HashSet<>();
		// Start building from sink
		SeepLogicalOperator op = (SeepLogicalOperator) slq.getSink();
		// Recursive method, with opsAlreadyInSchedule to detect already incorporated stages
		Set<Stage> stages = new HashSet<>();
		int stageId = 0;
		buildScheduleFromStage(null, op,  opsAlreadyInSchedule, slq, stages, stageId);
		ScheduleDescription sd = new ScheduleDescription(stages, slq.getAllOperators());
		return sd;
	}
	
	private void buildScheduleFromStage(Stage parent, SeepLogicalOperator slo,
			Set<Integer> opsAlreadyInSchedule, SeepLogicalQuery slq, Set<Stage> stages, int stageId) {
		// Check whether this op has already been incorporated to a stage and abort if so
		int opId = slo.getOperatorId();
		if(opsAlreadyInSchedule.contains(opId)) {
			// Create dependency with the stage governing opId in this case and return
			Stage dependency = stageResponsibleFor(opId);
			parent.dependsOn(dependency);
			return;
		}
		// Create new stage and dependency with parent
		Stage stage = new Stage(stageId);
		if(parent != null) {
			parent.dependsOn(stage);
		}
		stage = createStageFromLogicalOperator(stage, opsAlreadyInSchedule, slo);
		stages.add(stage);
		StageType type = stage.getStageType();

		// If we hit a source or unique stage, then configure Input and finish
		if(type.equals(StageType.SOURCE_STAGE) || type.equals(StageType.UNIQUE_STAGE)) {
			return;
		}
		
		// Update slo after stage creation
		slo = (SeepLogicalOperator) slq.getOperatorWithId(stage.getIdOfOperatorBoundingStage());
		
		// If multiple input explore for each
		if(stage.hasMultipleInput()) {
			for(UpstreamConnection uc : slo.upstreamConnections()) {
				SeepLogicalOperator upstreamOp = (SeepLogicalOperator) uc.getUpstreamOperator();
				stageId++;
				buildScheduleFromStage(stage, upstreamOp, opsAlreadyInSchedule, slq, stages, stageId);
			}
		// If not explore the previous op
		}
		else {
			SeepLogicalOperator upstreamOp = (SeepLogicalOperator)slo.upstreamConnections().get(0).getUpstreamOperator();
			stageId++;
			buildScheduleFromStage(stage, upstreamOp,  opsAlreadyInSchedule, slq, stages, stageId);
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
			
			// FIXME: no need to reason about stateful ops here. only whether they need or not shuffle
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
		for(DownstreamConnection dc : slo.downstreamConnections()) {
			if (dc.getDownstreamOperator() instanceof MarkerSink) {
				return true;
			}
		}
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
	
	/** Implement ScheduleManager interface **/
	
	@Override
	public void notifyStageStatus(StageStatusCommand ssc) {
		int stageId = ssc.getStageId();
		int euId = ssc.getEuId();
		Map<Integer, Set<DataReference>> results = ssc.getResultDataReference();
		StageStatusCommand.Status status = ssc.getStatus();
		List<RuntimeEvent> runtimeEvents = ssc.getRuntimeEvents();
		seWorker.newStageStatus(stageId, euId, results, status, runtimeEvents);
	}
	
	/** Methods to facilitate testing **/
	
	public void __initializeEverything(){
		seWorker.prepareForStart(null);
	}
	
	public ScheduleTracker __tracker_for_test(){
		return seWorker.__tracker_for_testing();
	}
	
	public Stage __get_next_stage_to_schedule_fot_test(){
		return seWorker.__next_stage_scheduler();
	}
	
	public void __reset_schedule() {
		seWorker.__reset_schedule();
	}
	
}
