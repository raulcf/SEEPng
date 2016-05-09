package uk.ac.imperial.lsds.seepmaster.scheduler.memorymanagement;


public enum MemoryManagementPolicyType {
	LRU(0),
	MDF(1);
	
	int type;
	
	MemoryManagementPolicyType(int type) {
		this.type = type;
	}
	
	public int ofType() {
		return type;
	}
}
