package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

import com.esotericsoftware.kryo.Kryo;

public class SchedulerEngineWorker implements Runnable {

	private SchedulingStrategy schedulingStrategy;
	private ScheduleTracker tracker;
	
	private InfrastructureManager inf;
	private Set<Connection> connections;
	private Comm comm;
	private Kryo k;
	
	private boolean work = true;
	
	public SchedulerEngineWorker(SchedulingStrategy schedulingStrategy, ScheduleTracker tracker, InfrastructureManager inf, Comm comm, Kryo k) {
		this.schedulingStrategy = schedulingStrategy;
		this.tracker = tracker;
		this.inf = inf;
		this.comm = comm;
		this.k = k;
	}
	
	public void setConnections(Set<Connection> connections) {
		this.connections = connections;
	}


	public void stop() {
		this.work = false;
	}
	
	@Override
	public void run() {
		while(work) {
			// Get next stage
			Stage nextStage = schedulingStrategy.next(tracker);
			MasterWorkerCommand esc = ProtocolCommandFactory.buildExecuteStageCommand(nextStage.getStageId());
			
			Set<Connection> euInvolved = getWorkersInvolvedInStage(nextStage);
			// Send stage to all workers and wait... (easy to get only a subset, since we have inf here)
			boolean success = comm.send_object_sync(esc, euInvolved, k);
			// Wait until stage is completed
			waitForNodes(nextStage, euInvolved);
		}
	}
	
	private Set<Connection> getWorkersInvolvedInStage(Stage stage) {
		// TODO: for now all workers execute all stages...
		
		return connections;
	}
	
	private void waitForNodes(Stage stage, Set<Connection> euInvolved) {
		
		// better to wait on a latch or something....
		Set<Integer> euIds = new HashSet<>();
		for(Connection c : euInvolved) {
			euIds.add(c.getId());
		}
		tracker.trackAndWait(stage, euIds);
	}
	
}
