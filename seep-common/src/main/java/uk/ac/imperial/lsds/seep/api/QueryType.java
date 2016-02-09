package uk.ac.imperial.lsds.seep.api;

public enum QueryType {
	
	SEEPLOGICALQUERY ((short)0),
	SCHEDULE((short)1);
	
	private short type;
	
	QueryType(short type) {
		this.type = type;
	}
	
	public short ofType() {
		return type;
	}
}
