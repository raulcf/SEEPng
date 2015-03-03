package uk.ac.imperial.lsds.seepworker.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class ProcessingEngineFactory {

	final private static Logger LOG = LoggerFactory.getLogger(ProcessingEngineFactory.class.getName());
	
	public static ProcessingEngine buildProcessingEngine(WorkerConfig wc){
		int engineType = wc.getInt(WorkerConfig.ENGINE_TYPE);
		if(engineType == ProcessingEngineType.SINGLE_THREAD.ofType()){
			LOG.info("Building processing engine of type: {}", "SINGLE_THREAD");
			return new SingleThreadProcessingEngine(wc);
		}
		return null;
	}
	
}
