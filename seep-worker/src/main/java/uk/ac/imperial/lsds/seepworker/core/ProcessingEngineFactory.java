package uk.ac.imperial.lsds.seepworker.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor.ConductorCallback;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

public class ProcessingEngineFactory {

	final private static Logger LOG = LoggerFactory.getLogger(ProcessingEngineFactory.class.getName());
	
	public static ProcessingEngine buildSingleTaskProcessingEngine(WorkerConfig wc, int id, SeepTask task, SeepState state, CoreInput coreInput, CoreOutput coreOutput, ConductorCallback callback, DataReferenceManager drm){
		int engineType = wc.getInt(WorkerConfig.ENGINE_TYPE);
		if(engineType == ProcessingEngineType.SINGLE_THREAD.ofType()){
			LOG.info("Building processing engine of type: {}", "SINGLE_THREAD");
			return new SingleThreadProcessingEngine(wc, id, task, state, coreInput, coreOutput, callback, CollectorType.SIMPLE, drm);
		}
		return null;
	}

	public static ProcessingEngine buildComposedTaskProcessingEngine(WorkerConfig wc, int id, SeepTask task, SeepState state, CoreInput coreInput, CoreOutput coreOutput, ConductorCallback callback, DataReferenceManager drm) {
		int engineType = wc.getInt(WorkerConfig.ENGINE_TYPE);
		if(engineType == ProcessingEngineType.SINGLE_THREAD.ofType()) {
			LOG.info("Building processing engine of type: {}", "SINGLE_THREAD");
			return new SingleThreadProcessingEngine(wc, id, task, state, coreInput, coreOutput, callback, CollectorType.COMPOSED_SEQUENTIAL_TASK, drm);
		}
		return null;
	}
	
}
