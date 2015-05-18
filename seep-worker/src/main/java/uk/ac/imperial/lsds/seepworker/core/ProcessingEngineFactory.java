package uk.ac.imperial.lsds.seepworker.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

public class ProcessingEngineFactory {

	final private static Logger LOG = LoggerFactory.getLogger(ProcessingEngineFactory.class.getName());
	
	public static ProcessingEngine buildProcessingEngine(WorkerConfig wc, int id, SeepTask task, SeepState state, CoreInput coreInput, CoreOutput coreOutput){
		int engineType = wc.getInt(WorkerConfig.ENGINE_TYPE);
		if(engineType == ProcessingEngineType.SINGLE_THREAD.ofType()){
			LOG.info("Building processing engine of type: {}", "SINGLE_THREAD");
			return new SingleThreadProcessingEngine(wc, id, task, state, coreInput, coreOutput);
		}
		return null;
	}

	public static ProcessingEngine buildAdHocProcessingEngine(WorkerConfig wc, CoreInput coreInput, CoreOutput coreOutput, ScheduleTask task) {
		return new AdHocProcessingEngine(wc, coreInput, coreOutput, task);
	}
	
}
