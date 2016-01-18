package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;

public class LocalSchedulerStageCommand implements CommandType {

	private int stageId;
	private Map<Integer, Set<DataReference>> inputDataReferences;
	private Map<Integer, Set<DataReference>> outputDataReferences;

	// Default constructor for Kryo
	public LocalSchedulerStageCommand() {}

	public LocalSchedulerStageCommand(int stageId, Map<Integer, Set<DataReference>> input,
			Map<Integer, Set<DataReference>> ouptut) {
		this.stageId = stageId;
		this.inputDataReferences = input;
		this.outputDataReferences = ouptut;
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
	 * @return the inputDataReferences
	 */
	public Map<Integer, Set<DataReference>> getInputDataReferences() {
		return inputDataReferences;
	}

	/**
	 * @return the outputDataReferences
	 */
	public Map<Integer, Set<DataReference>> getOutputDataReferences() {
		return outputDataReferences;
	}

}
