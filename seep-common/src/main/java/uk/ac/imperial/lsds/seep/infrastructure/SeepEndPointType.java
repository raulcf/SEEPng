package uk.ac.imperial.lsds.seep.infrastructure;

public enum SeepEndPointType {
	MASTER_CONTROL(0),
	WORKER_CONTROL(1),
	DATA(2);
	
	private int type;
	
	SeepEndPointType(int type) {
		this.type = type;
	}
	
	public int ofType() {
		return type;
	}
}
