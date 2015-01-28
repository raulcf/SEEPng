package uk.ac.imperial.lsds.seepworker.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessingEngineFactory {

	final private static Logger LOG = LoggerFactory.getLogger(ProcessingEngineFactory.class.getName());
	
	public static ProcessingEngine buildProcessingEngine(int type){
		if(type == ProcessingEngineType.SINGLE_THREAD.ofType()){
			LOG.info("Building processing engine of type: {}", "SINGLE_THREAD");
			return new SingleThreadProcessingEngine();
		}
		return null;
	}
	
}
