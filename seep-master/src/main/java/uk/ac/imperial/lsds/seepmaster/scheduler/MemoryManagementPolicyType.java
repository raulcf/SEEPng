package uk.ac.imperial.lsds.seepmaster.scheduler;

public enum MemoryManagementPolicyType {
	LRU(0, new LRUMemoryManagementPolicy()),
	MDF(1, new MDFMemoryManagementPolicy());
	
	int type;
	MemoryManagementPolicy mmp;
	
	MemoryManagementPolicyType(int type, MemoryManagementPolicy mmp) {
		this.type = type;
		this.mmp = mmp;
	}
	
	public int ofType() {
		return type;
	}
	
	public static MemoryManagementPolicy clazz(int type){
		for(MemoryManagementPolicyType sst : MemoryManagementPolicyType.values()){
			if(sst.ofType() == type){
				return sst.mmp;
			}
		}
		return null;
	}
}
