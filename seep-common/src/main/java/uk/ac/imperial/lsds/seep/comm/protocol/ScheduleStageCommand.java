package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;

public class ScheduleStageCommand implements CommandType {

	private int stageId;
	private Set<DataReference> inputDataReferences;
	private Set<DataReference> outputDataReferences;
	
	public ScheduleStageCommand() {}
	
	public ScheduleStageCommand(int stageId, Set<DataReference> input, Set<DataReference> output) {
		this.stageId = stageId;
		this.inputDataReferences = input;
		this.outputDataReferences = output;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.SCHEDULE_STAGE.type();
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public Set<DataReference> getInputDataReferences() {
		return inputDataReferences;
	}
	
	public Set<DataReference> getOutputDataReference() {
		return outputDataReferences;
	}

}
