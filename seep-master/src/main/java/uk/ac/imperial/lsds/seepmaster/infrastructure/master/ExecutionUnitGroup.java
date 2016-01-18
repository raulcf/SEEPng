package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import java.util.ArrayList;
import java.util.Random;

import uk.ac.imperial.lsds.seep.errors.TwoLevelSchedulerException;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public class ExecutionUnitGroup {
	
	private String group_ip;
	private ArrayList<ExecutionUnit> workers;
	private ExecutionUnit local_scheduler;
	
	//Maybe add Group Stage etc..
	
	public ExecutionUnitGroup(String group_ip){
		this.group_ip = group_ip;
		this.workers = new ArrayList<ExecutionUnit>();
	}
	
	public void addToExecutionGroup(ExecutionUnit unit){
		if(!belognsToGroup(unit))
				throw new TwoLevelSchedulerException("Attempted to add worker" + unit + "that does not belong to this group: "+ this.group_ip);
		workers.add(unit);
	}
	
	public boolean belognsToGroup(ExecutionUnit unit){
		return unit.getEndPoint().getIpString().compareToIgnoreCase(group_ip) == 0;
	}
	
	public void localSchedulerElect(){
			if(workers.size() < 2 ){
				throw new TwoLevelSchedulerException("Two Level Scheduling needs at least two workers per group! Not found for group: "+ this.group_ip);
			}
			Random generator = new Random();
			int randIndex = generator.nextInt(workers.size());
			this.local_scheduler = workers.remove(randIndex);
	}

	/**
	 * @return the group_ip
	 */
	public String getGroup_ip() {
		return group_ip;
	}

	/**
	 * @return the workers
	 */
	public ArrayList<ExecutionUnit> getWorkers() {
		return workers;
	}
	
	public ArrayList<EndPoint> getWorkerEndpoints() {
		ArrayList<EndPoint> toret = new ArrayList<EndPoint>();
		for (ExecutionUnit eu : this.workers)
			toret.add(eu.getEndPoint());
		return toret;
	}

	/**
	 * @return the local_scheduler
	 */
	public ExecutionUnit getLocal_scheduler() {
		return local_scheduler;
	}

	public String toString(){
		StringBuffer sb =new StringBuffer();
		sb.append("\nGroup IP: "+ this.group_ip +"\t");
		sb.append("Workers Size: "+ this.workers.size()+"\n");
		if(this.local_scheduler != null)
			sb.append("Local Scheduler: "+ this.local_scheduler.getId() + ", "+ this.local_scheduler.getEndPoint().getIpString()+"\n");
		else
			sb.append("Local Scheduler: NOT YET ELECTED\n");
		sb.append("Workers: \n");
		for(ExecutionUnit eu :this.workers)
			sb.append("\tID: "+ eu.getId() + ", "+ eu.getEndPoint().getIpString()+ " controlPort: "+eu.getEndPoint().getControlPort() +"\n");
		return sb.toString();
	}
}
