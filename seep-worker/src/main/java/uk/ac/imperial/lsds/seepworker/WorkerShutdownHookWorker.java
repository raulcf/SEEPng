package uk.ac.imperial.lsds.seepworker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.comm.WorkerMasterAPIImplementation;
import uk.ac.imperial.lsds.seepworker.core.Conductor;

public class WorkerShutdownHookWorker implements Runnable {

	final private static Logger LOG = LoggerFactory.getLogger(WorkerShutdownHookWorker.class);
	
	private final int workerId;
	
	private Conductor c;
	private Connection masterConn;
	private WorkerMasterAPIImplementation api;
	
	public WorkerShutdownHookWorker(int workerId, Conductor c, Connection masterConn, WorkerMasterAPIImplementation api) {
		this.workerId = workerId;
		this.c = c;
		this.masterConn = masterConn;
		this.api = api;
	}

	@Override
	public void run() {
		// Start emergency thread, in case the shutdown hook gets deadlocked with some resource
		this.emergencyHook();
		
		LOG.info("JVM is shutting down...");
		LOG.info("Worker is shutting down, sending bye message");
		String reason = "unknown";
		api.signalDeadWorker(masterConn, workerId, reason);
		c.stopProcessing();
		LOG.info("bye");
	}
	
	private void emergencyHook(){
		new Thread( new Runnable(){
			public void run(){
				try {
					// Wait 5 seconds, then kill the JVM
					synchronized(this) {
						Thread.currentThread().wait(5000);
					}
					Runtime.getRuntime().halt(0);
				}
				catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

}
