package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Set;

import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class LocalSchedulerStagesCommand implements CommandType {

	private int stageId;
	Set<Stage> stages;

	// Default constructor for Kryo
	public LocalSchedulerStagesCommand() {}

	public LocalSchedulerStagesCommand(int stageId, Set<Stage> stages) {
		this.stageId = stageId;
		this.stages = stages;
	}

	@Override
	public short type() {
		return MasterWorkerProtocolAPI.LOCAL_SCHEDULE.type();
	}

	/**
	 * @return the stageId
	 */
	public int getStageId() {
		return stageId;
	}
	
	/**
	 * @return the stages
	 */
	public Set<Stage> getStages() {
		return stages;
	}


}
