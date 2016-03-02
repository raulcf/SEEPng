package uk.ac.imperial.lsds.seep.api;

public class RuntimeEvent {

	private int type;
	
	private SpillToDiskRuntimeEvent stdre;
	
	public RuntimeEvent() { }
	
	public RuntimeEvent(RuntimeEventType ret) {
		int type = ret.type();
		if(type == RuntimeEventTypes.DATASET_SPILL_TO_DISK.ofType()) {
			this.stdre = (SpillToDiskRuntimeEvent)ret;
		}
	}
	
	public int type() {
		return type;
	}
	
	public SpillToDiskRuntimeEvent getSpillToDiskRuntimeEvent() {
		return stdre;
	}
	
}
