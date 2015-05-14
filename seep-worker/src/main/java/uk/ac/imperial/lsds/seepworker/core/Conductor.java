package uk.ac.imperial.lsds.seepworker.core;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.StatefulSeepTask;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInputFactory;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutputFactory;

public class Conductor {

	final private Logger LOG = LoggerFactory.getLogger(Conductor.class.getName());
	
	private WorkerConfig wc;
	private InetAddress myIp;
	private int id;
	private SeepLogicalQuery query;
	private Map<Integer, EndPoint> mapping;
	
	private List<DataStoreSelector> dataStoreSelectors;
	
	private CoreInput coreInput;
	private CoreOutput coreOutput;
	private ProcessingEngine engine;
	
	
	public Conductor(InetAddress myIp, WorkerConfig wc){
		this.myIp = myIp;
		this.wc = wc;
		engine = ProcessingEngineFactory.buildProcessingEngine(wc);
	}
	
	public void setQuery(int id, SeepLogicalQuery query, Map<Integer, EndPoint> mapping) {
		this.id = id;
		this.query = query;
		this.mapping = mapping;
	}
	
	public void materializeAndConfigureTask(){
		int opId = getOpIdLivingInThisEU(id);
		LogicalOperator o = query.getOperatorWithId(opId);
		LOG.info("Found LogicalOperator: {} mapped to this executionUnit: {} stateful: {}", o.getOperatorName(), 
				id, o.isStateful());
		
		SeepTask task = o.getSeepTask();
		LOG.info("Configuring local task: {}", task.toString());
		// set up state if any
		SeepState state = o.getState();
		if(o.isStateful()){
			LOG.info("Configuring state of local task: {}", state.toString());
			((StatefulSeepTask)task).setState(state);
		}
		// This creates one inputAdapter per upstream stream Id
		coreInput = CoreInputFactory.buildCoreInputForOperator(wc, o);
		// This creates one outputAdapter per downstream stream Id
		coreOutput = CoreOutputFactory.buildCoreOutputForOperator(wc, o, mapping);
		
		dataStoreSelectors = DataStoreSelectorFactory.buildDataStoreSelector(coreInput, 
				coreOutput, wc, o, myIp, wc.getInt(WorkerConfig.DATA_PORT));

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
	
	private int getOpIdLivingInThisEU(int id) {
		for(Entry<Integer, EndPoint> entry : mapping.entrySet()) {
			if(entry.getValue().getId() == id) return entry.getKey();
		}
		return -1;
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
		
}
