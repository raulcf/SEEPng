package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepPhysicalQuery;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.StatefulSeepTask;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInputFactory;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutputFactory;

public class Conductor {

	final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());
	
	private WorkerConfig wc;
	
	private int dataPort;
	private InetAddress myIp;
	
	private List<DataStoreSelector> dataStoreSelectors;
	
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
		for(DataStoreSelector dss : dataStoreSelectors) {
			dss.startSelector();
		}
		engine.start();
	}
	
	public void stopProcessing(){
		LOG.info("Stopping processing engine...");
		engine.stop();
		for(DataStoreSelector dss : dataStoreSelectors) {
			dss.stopSelector();
		}
		LOG.info("Stopping processing engine...OK");
	}
	
	public void deployPhysicalOperator(PhysicalOperator o, SeepPhysicalQuery query){
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
		
		dataStoreSelectors = DataStoreSelectorFactory.buildDataStoreSelector(coreInput, 
				coreOutput, wc, o, myIp, dataPort);

		// FIXME: this is ugly, why ns is special?
		for(DataStoreSelector dss : dataStoreSelectors) {
			if(dss instanceof EventAPI) coreOutput.setEventAPI((EventAPI)dss);
		}
		
		engine.setId(o.getOperatorId());
		engine.setTask(task);
		engine.setSeepState(state);
		engine.setCoreInput(coreInput);
		engine.setCoreOutput(coreOutput);
		
		// Initialize system
		LOG.info("Setting up task...");
		task.setUp(); // setup method of task
		LOG.info("Setting up task...OK");
		for(DataStoreSelector dss : dataStoreSelectors) {
			dss.initSelector();
		}
	}
		
}
