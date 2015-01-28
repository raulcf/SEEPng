package uk.ac.imperial.lsds.seepworker.core;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.input.InputAdapter;
import uk.ac.imperial.lsds.seepworker.core.input.InputAdapterReturnType;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.OutputAdapter;

public class SingleThreadProcessingEngine implements ProcessingEngine {

	final private Logger LOG = LoggerFactory.getLogger(SingleThreadProcessingEngine.class.getName());
	// TODO: move value to a property in workerconfig
	final private int MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS = 500;
	
	private boolean working = false;
	private Thread worker;
	
	private int id;
	private CoreInput coreInput;
	private CoreOutput coreOutput;
	
	private SeepTask task;
	private SeepState state;
	
	public SingleThreadProcessingEngine(){
		this.worker = new Thread(new Worker());
		this.worker.setName(this.getClass().getSimpleName());
	}
	
	@Override
	public void setId(int id) {
		this.id = id;
	}
	
	@Override
	public void setCoreInput(CoreInput coreInput) {
		this.coreInput = coreInput;
	}

	@Override
	public void setCoreOutput(CoreOutput coreOutput) {
		this.coreOutput = coreOutput;
	}
	
	@Override
	public void setTask(SeepTask task) {
		this.task = task;
	}
	
	@Override
	public void setSeepState(SeepState state) {
		this.state = state;
	}

	@Override
	public void start() {
		working = true;
		this.worker.start();
	}

	@Override
	public void stop() {
		working = false;
		// TODO: additional cleaning required
	}
	
	private class Worker implements Runnable{

		@Override
		public void run() {
			List<InputAdapter> inputAdapters = coreInput.getInputAdapters();
			LOG.info("Configuring SINGLETHREAD processing engine with {} inputAdapters", inputAdapters.size());
			Iterator<InputAdapter> it = inputAdapters.iterator();
			short one = InputAdapterReturnType.ONE.ofType();
			short many = InputAdapterReturnType.MANY.ofType();
			List<OutputAdapter> outputAdapters = coreOutput.getOutputAdapters();
			LOG.info("Configuring SINGLETHREAD processing engine with {} outputAdapters", outputAdapters.size());
			API api = new Collector(id, outputAdapters);
			while(working){
				while(it.hasNext()){
					InputAdapter ia = it.next();
					if(ia.returnType() == one){
						ITuple d = ia.pullDataItem(MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS);
						if(d != null){
							task.processData(d, api);
						}
					}
					else if(ia.returnType() == many){
						ITuple ld = ia.pullDataItems(MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS);
						if(ld != null){
							task.processDataGroup(ld, api);
						}
					}
					if(!it.hasNext()){
						it = inputAdapters.iterator();
					}
				}
				// If there are no input adapters, assume processData contain all necessary and give null input data
				LOG.info("About to call processData without data. Am I a source?");
				task.processData(null, api);
			}
		}
	}
}
