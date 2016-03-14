package uk.ac.imperial.lsds.seepworker.scheduler;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.protocol.LocalSchedulerStagesCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StageStatusCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.WorkerControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.engine.SchedulingStrategyType;

/**
 * @author pg1712@ic.ac.uk
 *
 */
public class LocalScheduleManager {
	
	final private Logger LOG = LoggerFactory.getLogger(LocalScheduleManager.class);
	
	private Set<EndPoint> workers;
	private SeepLogicalQuery slq;
	private Comm comm;
	
	// Local Scheduler machinery
	private ScheduleDescription scheduleDescription;
	private LocalScheduler seWorker;
	private Thread worker;
	private int myPort;


	public LocalScheduleManager(Set<EndPoint> groupWorkers, int myPort) {
		this.workers = groupWorkers;
		this.comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		this.myPort = myPort;
	}
	
	
	public void handleStartQuery() {
		LOG.info("Starting Local Scheduler...");
		worker.start();
	}
	
	public void handleStopQuery(){
		LOG.info("Stoping Local Scheduler...");
		try {
			worker.join();
			this.seWorker.stop();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void handleLocalStageCommand(LocalSchedulerStagesCommand lssc){
		int stageId = lssc.getStageId();
		LOG.debug("Local Scheduler Received StageID: {} with Priority: {}", stageId, lssc.getStages().iterator().next().getPriority());
//		LOG.debug("Stages INFO {}"+ lssc.getStages());
		seWorker.prepareForNewStageLocal(this.getWorkerConnection(), lssc.getStages(), slq);
		if(!worker.isAlive())
			this.handleStartQuery();
	}
	
	public void groupScheduleDeploy(ScheduleDeployCommand sdc,  SeepLogicalQuery slq){
		this.slq = slq;
		this.scheduleDescription = sdc.getSchedule();
		boolean suc = this.sendScheduleToGroupNodes(this.getWorkerConnection(), slq);
		LOG.info("Group Scheduled deploy done: {}. Waiting for Global scheduler Stages...", suc);
		
		// Initialize the schedulerThread
		seWorker = new LocalScheduler(scheduleDescription,
				SchedulingStrategyType.clazz(0), comm, KryoFactory.buildKryoForMasterWorkerProtocol(), this.getWorkerConnection());
		worker = new Thread(seWorker);
		worker.setName("LocalScheduleManager");
	}
	
	private boolean sendScheduleToGroupNodes(Set<Connection> connections, SeepLogicalQuery slq){
		LOG.info("Sending Group Schedule Deploy Command...");
		// Send physical query to all Group Worker nodes
		MasterWorkerCommand scheduleDeploy = ProtocolCommandFactory.buildScheduleDeployCommand(slq, scheduleDescription, myPort);
		boolean success = comm.send_object_sync(scheduleDeploy, connections, KryoFactory.buildKryoForMasterWorkerProtocol());
		return success;
	}

	
	public Set<Connection> getWorkerConnection(){
		Set<Connection> connections = new HashSet<Connection>();
		for(EndPoint e : this.getWorkers())
			connections.add(new Connection(new WorkerControlEndPoint(e.getId(), e.getIpString(), e.getPort())));
		return connections;
	}
	/**
	 * @return the workers
	 */
	public Set<EndPoint> getWorkers() {
		return workers;
	}

	/**
	 * @param workers the workers to set
	 */
	public void setWorkers(Set<EndPoint> workers) {
		this.workers = workers;
	}

	/**
	 * @return the scheduleDescription
	 */
	public ScheduleDescription getScheduleDescription() {
		return scheduleDescription;
	}

	/**
	 * @param scheduleDescription the scheduleDescription to set
	 */
	public void setScheduleDescription(ScheduleDescription scheduleDescription) {
		this.scheduleDescription = scheduleDescription;
	}
	/*
	 * Similar to QueryManager Schedule notification
	 */
	public void notifyStageStatus(StageStatusCommand ssc) {
		int stageId = ssc.getStageId();
		int euId = ssc.getEuId();
		Map<Integer, Set<DataReference>> results = ssc.getResultDataReference();
		StageStatusCommand.Status status = ssc.getStatus();
		seWorker.newStageStatus(stageId, euId, results, status);
	}

}
