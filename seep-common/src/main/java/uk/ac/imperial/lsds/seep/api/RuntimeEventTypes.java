package uk.ac.imperial.lsds.seep.api;

public enum RuntimeEventTypes {
	
	DATASET_SPILL_TO_DISK(0);
	
	private int type;
	
	RuntimeEventTypes(int type) {
		this.type = type;
	}
	
	int ofType() {
		return type;
	}
}
