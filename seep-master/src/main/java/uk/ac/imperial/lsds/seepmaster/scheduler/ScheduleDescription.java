package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.Set;

public class ScheduleDescription {

	private Set<Stage> stages;
	
	public ScheduleDescription(Set<Stage> stages) {
		this.stages = stages;
	}
	
	public Set<Stage> getStages(){
		return stages;
	}
	
}
