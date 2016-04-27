package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.core.DatasetMetadata;
import uk.ac.imperial.lsds.seep.core.DatasetMetadataPackage;

public class StageStatusCommand implements CommandType {

	private int stageId;
	private int euId;
	private Status status;
	private Map<Integer, Set<DataReference>> resultDataReference;
	private List<RuntimeEvent> runtimeEvents;
	private DatasetMetadataPackage managedDatasets;
	
	public StageStatusCommand() {}
	
	public StageStatusCommand(int stageId, int euId, Status status, Map<Integer, Set<DataReference>> producedOutput, List<RuntimeEvent> runtimeEvents, DatasetMetadataPackage managedDatasets) {
		this.stageId = stageId;
		this.euId = euId;
		this.status = status;
		this.resultDataReference = producedOutput;
		this.runtimeEvents = runtimeEvents;
		this.managedDatasets = managedDatasets;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.STAGE_STATUS.type();
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public int getEuId() {
		return euId;
	}
	
	public Status getStatus() {
		return status;
	}
	
	public Map<Integer, Set<DataReference>> getResultDataReference() {
		return resultDataReference;
	}
	
	public List<RuntimeEvent> getRuntimeEvents() {
		return runtimeEvents;
	}
	
	public DatasetMetadataPackage getManagedDatasets() {
		return managedDatasets;
	}
	
	public enum Status {
		FAIL,
		OK,
	}

}
