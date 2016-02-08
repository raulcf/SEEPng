package uk.ac.imperial.lsds.seep.api;

public enum QueryExecutionMode {
	ALL_MATERIALIZED((short) 0),
	ALL_SCHEDULED((short) 1),
	AUTOMATIC_HYBRID((short) 2),
	TWO_LEVEL_SCHELUDED((short) 3);
	
	private short type;
	
	QueryExecutionMode(short type){
		this.type = type;
	}
	
	public short ofType(){
		return type;
	}
}
