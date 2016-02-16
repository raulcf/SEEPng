package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataReference.ServeMode;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.StatefulSeepTask;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand.Status;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.infrastructure.DataEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.comm.NetworkSelector;
import uk.ac.imperial.lsds.seepworker.comm.ControlAPIImplementation;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInputFactory;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutputFactory;

import com.esotericsoftware.kryo.Kryo;

public class Conductor {

	final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());
	
	private WorkerConfig wc;
	private InetAddress myIp;
	private ControlAPIImplementation masterApi;
	private Connection masterConn;
	private Comm comm;
	private Kryo k;
	private int id;
	private SeepLogicalQuery query;
	private Map<Integer, SeepEndPoint> mapping;
	// TODO: these two are only specific to materialise tasks
	private Map<Integer, Map<Integer, Set<DataReference>>> inputs;
	private Map<Integer, Map<Integer, Set<DataReference>>> outputs;
	
	private List<DataStoreSelector> dataStoreSelectors;
	
	private CoreInput coreInput;
	private CoreOutput coreOutput;
	private ProcessingEngine engine;
	private DataReferenceManager drm;
	private CountDownLatch registerFlag;
	
	// Keep stage - scheduleTask
	private Map<Stage, ScheduleTask> scheduleTasks;
	private ScheduleDescription sd;
	
	public Conductor(InetAddress myIp, ControlAPIImplementation masterApi, Connection masterConn, WorkerConfig wc, Comm comm, DataReferenceManager drm){
		this.myIp = myIp;
		this.masterApi = masterApi;
		this.masterConn = masterConn;
		this.wc = wc;
		this.scheduleTasks = new HashMap<>();
		this.drm = drm;
		this.comm = comm;
		this.k = KryoFactory.buildKryoForWorkerWorkerProtocol();
		this.registerFlag = new CountDownLatch(1);
	}
	
	public void setQuery(int id, SeepLogicalQuery query, Map<Integer, SeepEndPoint> mapping, Map<Integer, Map<Integer, Set<DataReference>>> inputs, Map<Integer, Map<Integer, Set<DataReference>>> outputs) {
		this.id = id;
		this.query = query;
		this.mapping = mapping;
		this.inputs = inputs;
		this.outputs = outputs;
	}
	
	public void materializeAndConfigureTask() {
		int opId = getOpIdLivingInThisEU(id);
		LogicalOperator o = query.getOperatorWithId(opId);
		LOG.info("Found LogicalOperator: {} mapped to this executionUnit: {} stateful: {}", o.getOperatorName(), 
				id, o.isStateful());
		
		SeepTask task = o.getSeepTask();
		LOG.info("Configuring local task: {}", task.toString());
		// set up state if any
		SeepState state = o.getState();
		if (o.isStateful()) {
			LOG.info("Configuring state of local task: {}", state.toString());
			((StatefulSeepTask)task).setState(state);
		}
		// This creates one inputAdapter per upstream stream Id
		Map<Integer, Set<DataReference>> input = inputs.get(o.getOperatorId());
		Map<Integer, Set<DataReference>> output = outputs.get(o.getOperatorId());
		Map<Integer, ConnectionType> connTypeInformation = getInputConnectionType(o);
		coreInput = CoreInputFactory.buildCoreInputFor(wc, drm, input, connTypeInformation);
		coreOutput = CoreOutputFactory.buildCoreOutputFor(wc, drm, output);
		
		// Specialized data selectors
		dataStoreSelectors = DataStoreSelectorFactory.buildDataStoreSelector(coreInput, 
				coreOutput, wc, o, myIp, wc.getInt(WorkerConfig.DATA_PORT));
		
		// Share selectors with DRM so that it can serve data directly
		drm.setDataStoreSelectors(dataStoreSelectors);
		// Register in DRM all DataReferences managed
		for(Set<DataReference> drefs : output.values()) {
			for(DataReference dr : drefs) {
				drm.registerDataReferenceInCatalogue(dr);
			}
		}
		registerFlag.countDown();

		int id = o.getOperatorId();
		
		engine = ProcessingEngineFactory.buildSingleTaskProcessingEngine(wc, id, task, state, coreInput, coreOutput, makeContinuousConductorCallback());
		
		// Initialize system
		LOG.info("Setting up task...");
		task.setUp(); // setup method of task
		LOG.info("Setting up task...OK");
		for(DataStoreSelector dss : dataStoreSelectors) {
			dss.initSelector();
		}
		
		// Make sure selectors are initialised, then request connections
		coreInput.requestInputConnections(comm, k, myIp);
	}

	public void configureScheduleTasks(int id, ScheduleDescription sd, SeepLogicalQuery slq) {
		this.id = id;
		this.query = slq;
		this.sd = sd;
		LOG.info("Configuring environment for scheduled operation...");
		// Create ScheduleTask for every stage
		Set<Stage> stages = sd.getStages();
		LOG.info("Physical plan with {} stages", stages.size());
		for(Stage s : stages) {
			ScheduleTask st = ScheduleTask.buildTaskFor(id, s, slq);
			st.setUp();
			scheduleTasks.put(s, st);
		}
	}
	
	public void scheduleTask(int stageId, Map<Integer, Set<DataReference>> input, Map<Integer, Set<DataReference>> output) {
		Stage s = sd.getStageWithId(stageId);
		ScheduleTask task = this.scheduleTasks.get(s);
		LOG.info("Scheduling Stage:Task -> {}:{}", s.getStageId(), task.getEuId());
		
		// TODO: fix this, how useful is to configure this?
		Map<Integer, ConnectionType> connTypeInformation = new HashMap<>();
		for(Integer i : input.keySet()) {
			connTypeInformation.put(i, ConnectionType.ONE_AT_A_TIME);
		}
		
		coreInput = CoreInputFactory.buildCoreInputFor(wc, drm, input, connTypeInformation);
		if(output.size() == 0) {
			Schema expectedSchema = input.entrySet().iterator().next().getValue().iterator().next().getDataStore().getSchema();
			// FIXME: assumption, same schema as input -> will change once SINKs have also schemas
			output = createOutputForTask(s, expectedSchema);
		}
		coreOutput = CoreOutputFactory.buildCoreOutputFor(wc, drm, output);
		
		// Make sure that NetworkSelector is listening for input connections
		// FIXME: Note this is not reusable!! Can we make NetworkSelector a service rather than a
		// configure on-demand thing?
		if (coreInput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)) {
			NetworkSelector ns = DataStoreSelectorFactory.configureNetworkSelector(coreInput, 
				wc, stageId, myIp, wc.getInt(WorkerConfig.DATA_PORT));
			ns.initSelector();
			ns.startSelector();
		}
		
		// Request (possibly) remote chunks in case of scheduling a shuffled stage
		if(s.hasPartitionedState()) {
			// We pass our info---as the target EndPoint of the comm---and the 
			// workers will push their data to us
			coreInput.requestInputConnections(comm, k, myIp);
		}

		SeepState state = null;
		
		// probably pass to the callback here all info to talk with master
		ProcessingEngine engine = ProcessingEngineFactory.buildComposedTaskProcessingEngine(wc, 
				s.getStageId(), task, state, coreInput, 
				coreOutput, makeConductorCallbackForScheduleStage(stageId, id, output));
		engine.start();
	}
	
	private Map<Integer, Set<DataReference>> createOutputForTask(Stage s, Schema schema) {
		// Master did not assign output, so we need to create it here
		// This basically depends on how many outputs we need to generate
		Map<Integer, Set<DataReference>> output = new HashMap<>();
		
		if(s.hasDependantWithPartitionedStage()) {
			// create a DR per partition, that are managed
			// TODO: how to get the number of partitions
			int numPartitions = wc.getInt(WorkerConfig.SHUFFLE_NUM_PARTITIONS);
			int streamId = 0;
			Set<DataReference> drefs = new HashSet<>();
			// TODO: create a DR per partition and assign the partitionSeqId
			for(int i = 0; i < numPartitions; i++) {
				DataStore dataStore = new DataStore(schema, DataStoreType.IN_MEMORY);
				DataEndPoint dep = new DataEndPoint(id, wc.getString(WorkerConfig.WORKER_IP), wc.getInt(WorkerConfig.DATA_PORT));
				DataReference dr = null;
				int partitionId = i;
				dr = DataReference.makeManagedAndPartitionedDataReference(dataStore, dep, ServeMode.STORE, partitionId);
				drefs.add(dr);
			}
			output.put(streamId, drefs);
		}
		else {
			// create a single DR, that is managed
			int streamId = 0;
			Set<DataReference> drefs = new HashSet<>();
			DataStore dataStore = new DataStore(schema, DataStoreType.IN_MEMORY);
			DataEndPoint dep = new DataEndPoint(id, myIp.toString(), wc.getInt(WorkerConfig.DATA_PORT));
			DataReference dr = null;
			// TODO: is this enough?
			if(s.getStageType().equals(StageType.SINK_STAGE)) {
				dr = DataReference.makeSinkExternalDataReference(dataStore);
			}
			else {
				dr = DataReference.makeManagedDataReference(dataStore, dep, ServeMode.STORE);
			}
			drefs.add(dr);
			output.put(streamId, drefs);
		}
		return output;
	}
	
	public void startProcessing(){
		LOG.info("Starting processing engine...");
		for(DataStoreSelector dss : dataStoreSelectors) {
			dss.startSelector();
		}
		engine.start();
	}
	
	public void stopProcessing(){
		LOG.info("Stopping processing engine...");
		engine.stop();
		for(OBuffer output: coreOutput.getBuffers().values()) {
			output.flush();
		}
		for(DataStoreSelector dss : dataStoreSelectors) {
			dss.stopSelector();
		}
		LOG.info("Stopping processing engine...OK");
	}
	
	private Map<Integer, ConnectionType> getInputConnectionType(LogicalOperator o) {
		Map<Integer, ConnectionType> ct = new HashMap<>();
		for(UpstreamConnection uc : o.upstreamConnections()) {
			ct.put(uc.getStreamId(), uc.getConnectionType());
		}
		return ct;
	}

	private int getOpIdLivingInThisEU(int id) {
		for(Entry<Integer, SeepEndPoint> entry : mapping.entrySet()) {
			if(entry.getValue().getId() == id) return entry.getKey();
		}
		return -1;
	}
	
	public ConductorCallback makeContinuousConductorCallback() {
		return new ConductorCallback(true);
	}
	
	public ConductorCallback makeConductorCallbackForScheduleStage(int stageId, int euId, Map<Integer, Set<DataReference>> output) {
		return new ConductorCallback(false, stageId, euId, output);
	}
	
	class ConductorCallback {

		private boolean continuousTask;
		private int stageId;
		private int euId;
		private Map<Integer, Set<DataReference>> refToProducedOutput;
		
		private ConductorCallback(boolean continuousTask) {
			this.continuousTask = continuousTask;
		}
		
		private ConductorCallback(boolean continuousTask, int stageId, int euId, Map<Integer, Set<DataReference>> output) {
			this.continuousTask = continuousTask;
			this.stageId = stageId;
			this.euId = euId;
			this.refToProducedOutput = output;
		}
		
		public boolean isContinuousTask() {
			return continuousTask;
		}

		public void notifyOk() {
			masterApi.scheduleTaskStatus(masterConn, stageId, euId, Status.OK, refToProducedOutput);
		}
		
	}

	// FIXME: refactor, check where to place this method, along with the entire communication with datarefmanager
	// FIXME: do we need a separate entity for this?
	public void serveData(int dataRefId, DataEndPoint ep) {
		try {
			registerFlag.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Make sure DRM manages this DataReferenceId
		DataReference dr = drm.doesManageDataReference(dataRefId);
		if (dr == null) {
			// FIXME: error
			LOG.error("DataRefernece is null!!!");
			System.exit(-1);
		}
		drm.serveDataSet(coreOutput, dr, ep);
	}
		
}
