package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;

public class ScheduleStageCommand implements CommandType {

	private int stageId;
	private Map<Integer, Set<DataReference>> inputDataReferences;
	private Map<Integer, Set<DataReference>> outputDataReferences;
	private List<Integer> rankedDatasets;
	
	public ScheduleStageCommand() {}
	
	public ScheduleStageCommand(int stageId, Map<Integer, Set<DataReference>> input, Map<Integer, Set<DataReference>> ouptut, List<Integer> rankedDatasets) {
		this.stageId = stageId;
		this.inputDataReferences = input;
		this.outputDataReferences = ouptut;
		this.rankedDatasets = rankedDatasets;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.SCHEDULE_STAGE.type();
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public Map<Integer, Set<DataReference>> getInputDataReferences() {
		return inputDataReferences;
	}
	
	public Map<Integer, Set<DataReference>> getOutputDataReference() {
		return outputDataReferences;
	}
	
	public List<Integer> getRankedDatasets() {
		return this.rankedDatasets;
	}

}
