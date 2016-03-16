package uk.ac.imperial.lsds.seep.api;

public class SpillToDiskRuntimeEvent implements RuntimeEventType {

	private int datasetId;
	
	@Override
	public int type() {
		return RuntimeEventTypes.DATASET_SPILL_TO_DISK.ofType();
	}
	
	public SpillToDiskRuntimeEvent(int datasetId) {
		this.datasetId = datasetId;
	}
	
	public int getDatasetId() {
		return datasetId;
	}

}
