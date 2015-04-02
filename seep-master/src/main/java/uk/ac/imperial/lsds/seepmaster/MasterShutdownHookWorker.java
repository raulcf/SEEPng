package uk.ac.imperial.lsds.seepmaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterShutdownHookWorker implements Runnable {

	final private static Logger LOG = LoggerFactory.getLogger(MasterShutdownHookWorker.class);
	
	@Override
	public void run() {
		LOG.info("JVM is shutting down...");
		LOG.info("Closing Master...");
		// TODO: bookkeping tasks...
	}

}
