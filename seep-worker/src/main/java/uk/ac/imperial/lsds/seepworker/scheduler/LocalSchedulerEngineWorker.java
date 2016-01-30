package uk.ac.imperial.lsds.seepworker.scheduler;


import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.IOComm;
import uk.ac.imperial.lsds.seep.comm.protocol.LocalSchedulerStageCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.protocol.ScheduleDeployCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.StartQueryCommand;
import uk.ac.imperial.lsds.seep.comm.serialization.JavaSerializer;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.WorkerControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

/**
 * @author pg1712
 *
 */
public class LocalSchedulerEngineWorker { //implements Runnable{
	
	final private Logger LOG = LoggerFactory.getLogger(LocalSchedulerEngineWorker.class);
	
	private Set<EndPoint> workers;
	private ScheduleDescription scheduleDescription;
	

	public LocalSchedulerEngineWorker(Set<EndPoint> groupWorkers) {
		this.workers = groupWorkers;
	}
	
	
//	@Override
//	public void run() {
//		// TODO Auto-generated method stub
//		
//	}
	public boolean handleStartQuery(StartQueryCommand sqc) {
		// Send start query command to all Group Worker nodes
		Comm comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		MasterWorkerCommand startQuery = ProtocolCommandFactory.buildStartQueryCommand();
		boolean success = comm.send_object_sync(startQuery, this.getWorkerConnection(),
				KryoFactory.buildKryoForMasterWorkerProtocol());
		return success;
	}

	public void handleLocalStageCommand(LocalSchedulerStageCommand lssc){
		int stageId = lssc.getStageId();
		
	}
	
	public void groupScheduleDeploy(ScheduleDeployCommand sdc,  SeepLogicalQuery slq){
		this.scheduleDescription = sdc.getSchedule();
		boolean suc = this.sendScheduleToGroupNodes(this.getWorkerConnection(), slq);
		LOG.info("Group Scheduled deploy done: {}. Waiting for master commands...", suc);
	}
	
	private boolean sendScheduleToGroupNodes(Set<Connection> connections, SeepLogicalQuery slq){
		LOG.info("Sending Group Schedule Deploy Command");
		// Send physical query to all Group Worker nodes
		Comm comm = new IOComm(new JavaSerializer(), Executors.newCachedThreadPool());
		MasterWorkerCommand scheduleDeploy = ProtocolCommandFactory.buildScheduleDeployCommand(slq, scheduleDescription);
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
	

}
