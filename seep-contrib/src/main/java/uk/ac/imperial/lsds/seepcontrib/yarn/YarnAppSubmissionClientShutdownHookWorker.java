package uk.ac.imperial.lsds.seepcontrib.yarn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnAppSubmissionClientShutdownHookWorker implements Runnable {
    
    final private static Logger LOG = LoggerFactory.getLogger(YarnAppSubmissionClientShutdownHookWorker.class);
    
	@Override
	public void run() {
	    LOG.info("SeepYarnAppSubmissionClient is shutting down...");
	}

}
