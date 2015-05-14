package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.Source;
import uk.ac.imperial.lsds.seep.api.StatefulSeepTask;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seepcontrib.hdfs.comm.HdfsSelector;
import uk.ac.imperial.lsds.seepcontrib.hdfs.config.HdfsConfig;
import uk.ac.imperial.lsds.seepcontrib.kafka.comm.KafkaSelector;
import uk.ac.imperial.lsds.seepcontrib.kafka.config.KafkaConfig;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.comm.NetworkSelector;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInputFactory;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutputFactory;

public class Conductor {

	final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());
	
	private WorkerConfig wc;
	
	private int dataPort;
	private InetAddress myIp;
	private NetworkSelector ns;
	private FileSelector fs;
	private KafkaSelector ks;
	private HdfsSelector hs;
	
	private PhysicalOperator o;
	private CoreInput coreInput;
	private CoreOutput coreOutput;
	private ProcessingEngine engine;
	
	private SeepTask task;
	private SeepState state;
	
	public Conductor(InetAddress myIp, WorkerConfig wc){
		this.myIp = myIp;
		this.wc = wc;
		this.dataPort = wc.getInt(WorkerConfig.DATA_PORT);
		engine = ProcessingEngineFactory.buildProcessingEngine(wc);
	}
	
	public void startProcessing(){
		LOG.info("Starting processing engine...");
		if(ns != null) ns.startNetworkSelector();
		if(fs != null) fs.startFileSelector();
		if(ks != null) ks.startKafkaSelector();
		if(hs != null) hs.startHdfsSelector();
		engine.start();
	}
	
	public void stopProcessing(){
		LOG.info("Stopping processing engine...");
		engine.stop();
		
		if(ns != null) ns.stopNetworkSelector();
		if(fs != null) fs.stopFileSelector();
		if(ks != null) ks.stopKafkaSelector();
		if(hs != null) hs.stopHdfsSelector();
		LOG.info("Stopping processing engine...OK");
	}
	
	public void deployPhysicalOperator(PhysicalOperator o, PhysicalSeepQuery query){
		this.o = o;
		this.task = o.getSeepTask();
		LOG.info("Configuring local task: {}", task.toString());
		// set up state if any
		if(o.isStateful()){
			this.state = o.getState();
			LOG.info("Configuring state of local task: {}", state.toString());
			((StatefulSeepTask)task).setState(state);
		}
		// This creates one inputAdapter per upstream stream Id
		coreInput = CoreInputFactory.buildCoreInputForOperator(wc, o);
		// This creates one outputAdapter per downstream stream Id
		coreOutput = CoreOutputFactory.buildCoreOutputForOperator(wc, o, query);
		
		this.ns = maybeConfigureNetworkSelector();
		this.fs = maybeConfigureFileSelector();
		this.ks = maybeConfigureKafkaSelector();
		System.out.println("Here is operator:"+o.getOperatorId()+"=============");
		this.hs = maybeConfigureHdfsSelector();
		
		coreOutput.setEventAPI(ns);
		
		engine.setId(o.getOperatorId());
		engine.setTask(task);
		engine.setSeepState(state);
		engine.setCoreInput(coreInput);
		engine.setCoreOutput(coreOutput);
		
		// Initialize system
		LOG.info("Setting up task...");
		task.setUp(); // setup method of task
		LOG.info("Setting up task...OK");
		if(ns != null) ns.initNetworkSelector(); // start network selector, if any
	}
	
	private NetworkSelector maybeConfigureNetworkSelector(){
		NetworkSelector ns = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)){
			LOG.info("Configuring networkSelector for input");
			ns = new NetworkSelector(wc, o.getOperatorId(), coreInput.getInputAdapterProvider());
			ns.configureAccept(myIp, dataPort);
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.NETWORK)){
			LOG.info("Configuring networkSelector for output");
			if(ns == null) ns = new NetworkSelector(wc, o.getOperatorId(), coreInput.getInputAdapterProvider());
			Set<OutputBuffer> obufs = coreOutput.getOutputBuffers();
			ns.configureConnect(obufs);
		}
		return ns;
	}
	
	private FileSelector maybeConfigureFileSelector(){
		FileSelector fs = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.FILE)){
			fs = new FileSelector(wc);
			Map<Integer, DataStore> fileOrigins = new HashMap<>();
			for(UpstreamConnection uc : o.upstreamConnections()){
				int opId = uc.getUpstreamOperator().getOperatorId();
				if(uc.getDataOriginType() == DataStoreType.FILE) {
					fileOrigins.put(opId, uc.getDataOrigin());
				}
			}
			fs.configureAccept(fileOrigins, coreInput.getInputAdapterProvider());
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.FILE)){
			throw new NotImplementedException("not implemented yet...");
		}
		return fs;
	}
	
	private KafkaSelector maybeConfigureKafkaSelector(){
		KafkaSelector ks = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.KAFKA)){
			KafkaConfig kc = new KafkaConfig( o.upstreamConnections().get(0).getDataOrigin().getConfig() );
			LOG.info("Configuring kafkaSelector for input");
			ks = new KafkaSelector(kc.getString(KafkaConfig.BASE_TOPIC), kc.getString(KafkaConfig.ZOOKEEPER_CONNECT),
					kc.getString(KafkaConfig.CONSUMER_GROUP_ID), coreInput.getInputAdapterProvider());			
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.KAFKA)){
			// Not needed
		}
		return ks;
	}
	
	private HdfsSelector maybeConfigureHdfsSelector(){
		HdfsSelector hs = null;
		if(coreInput.requiresConfigureSelectorOfType(DataStoreType.HDFS)){
			int headroom = wc.getInt(WorkerConfig.BATCH_SIZE) * 2;
			hs = new HdfsSelector(headroom);
			Map<Integer, DataStore> fileOrigins = new HashMap<>();
			for(UpstreamConnection uc : o.upstreamConnections()){
				int opId = uc.getUpstreamOperator().getOperatorId();
				if(uc.getDataOriginType() == DataStoreType.HDFS){
					if(uc.getUpstreamOperator().getSeepTask() instanceof Source)
						hs.source();
					LOG.info(uc.getUpstreamOperator().getOperatorId()+"-"+o.getOperatorId());
					fileOrigins.put(opId, uc.getDataOrigin());
				}
			}
			hs.configureAccept(fileOrigins, coreInput.getInputAdapterProvider());
		}
		if(coreOutput.requiresConfigureSelectorOfType(DataStoreType.HDFS)){
			//TODO: implement for output to HDFS
		}
		return hs;
	}
	
	public void plugSeepTask(SeepTask task){
		// TODO: plug and play. this will do stuff with input and output and then delegate the call to engine
		// this pattern should be the default in this conductor controller
	}
	
}
