package uk.ac.imperial.lsds.seep.api;

public enum RuntimeEventTypes {
	
	DATASET_SPILL_TO_DISK(0),
	NOTIFY_END_LOOP(1);
	
	private int type;
	
	RuntimeEventTypes(int type) {
		this.type = type;
	}
	
	int ofType() {
		return type;
	}
}
