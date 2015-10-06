package uk.ac.imperial.lsds.seep.scheduler;

import java.util.Set;

import uk.ac.imperial.lsds.seep.util.Utils;

public class ScheduleDescription {

	private Set<Stage> stages;
	
	public ScheduleDescription() { }
	
	public ScheduleDescription(Set<Stage> stages) {
		this.stages = stages;
	}
	
	public Set<Stage> getStages() {
		return stages;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Schedule with "+stages.size()+" stages");
		sb.append(Utils.NL);
		return sb.toString();
	}

	public Stage getStageWithId(int stageId) {
		for(Stage s : stages) {
			if(s.getStageId() == stageId){
				return s;
			}
		}
		return null;
	}
	
}
