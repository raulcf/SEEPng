package uk.ac.imperial.lsds.seepworker.scheduler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;

/**
 * @author pg1712
 *
 */
public class LocalSchedulerEngineWorker implements Runnable{
	
	final private Logger LOG = LoggerFactory.getLogger(LocalSchedulerEngineWorker.class);
	
	private Set<EndPoint> workers;
	private ScheduleDescription scheduleDescription;
	

	public LocalSchedulerEngineWorker(Set<EndPoint> groupWorkers) {
		this.workers = groupWorkers;
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
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
