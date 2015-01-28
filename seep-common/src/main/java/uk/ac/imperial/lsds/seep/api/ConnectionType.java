package uk.ac.imperial.lsds.seep.api;

public enum ConnectionType {
	
	ONE_AT_A_TIME((short)0, "ONE-AT-A-TIME"),
	BATCH((short)1, "BATCH"),
	WINDOW((short)2, "WINDOW"),
	ORDERED((short)3, "ORDERED"),
	UPSTREAM_SYNC_BARRIER((short)4, "UPSTREAM-SYNC-BARRIER");
	
	private short type;
	private String name;
	
	ConnectionType(short type, String name){
		this.type = type;
		this.name = name;
	}
	
	public short ofType(){
		return type;
	}
	
	public String withName(){
		return name;
	}
}