package uk.ac.imperial.lsds.seepworker.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.DataItem;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

public class AdHocProcessingEngine implements ProcessingEngine {

	private final int MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS;
	private CoreInput coreInput;
	private CoreOutput coreOutput;
	private ScheduleTask task;
	private Thread worker;
	
	public AdHocProcessingEngine(WorkerConfig wc, CoreInput coreInput, CoreOutput coreOutput, ScheduleTask task) {
		this.MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS = wc.getInt(WorkerConfig.MAX_WAIT_TIME_PER_INPUTADAPTER_MS);
		this.coreInput = coreInput;
		this.coreOutput = coreOutput;
		this.task = task;
		this.worker = new Thread(new AdHocProcessingWorker());
	}

	@Override
	public void start() {
		worker.start();
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}
	
	class AdHocProcessingWorker implements Runnable {

		@Override
		public void run() {
			// while there are input data references left keep reading and processing
			List<InputAdapter> inputAdapters = coreInput.getInputAdapters();
			Iterator<InputAdapter> it = inputAdapters.iterator();
			task.configureScheduleTaskLazily(coreOutput.getOutputAdapters());
			while(it.hasNext()) {
				InputAdapter ia = it.next();
				DataItem di = ia.pullDataItem(MAX_BLOCKING_TIME_PER_INPUTADAPTER_MS);
				if(di != null){
					boolean consume = true;
					while(consume) {
						ITuple d = di.consume();
						if(d != null) {
							task.triggerProcessingPipeline(d);
						} else consume = false;
					}
				}
			}
			
			// then just return results and notify
			// TODO: notify processing is done, so scheduleTask is done, and point out to the datareferences
			Map<Integer, Set<DataReference>> producedOutput = new HashMap<>();
			for(OutputAdapter oa : coreOutput.getOutputAdapters()){
				Set<DataReference> outputResults = oa.getOutputDataReference();
				producedOutput.put(oa.getStreamId(), outputResults);
			}
			
			task.notifyStatusOk(producedOutput);
		}
	}

}
