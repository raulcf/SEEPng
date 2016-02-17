package uk.ac.imperial.lsds.seep.infrastructure;

public enum SeepEndPointType {
	
	CONTROL(0),
	DATA(1);
	
	private int type;
	
	SeepEndPointType(int type) {
		this.type = type;
	}
	
	public int ofType() {
		return type;
	}
}
