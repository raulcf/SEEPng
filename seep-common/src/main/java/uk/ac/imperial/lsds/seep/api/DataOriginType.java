package uk.ac.imperial.lsds.seep.api;


public enum DataOriginType {
	
	NETWORK((short)0), 
	FILE((short)1),
	IPC((short)2), // ??
	RDD((short)3),
	KAFKA((short)4),
	HDFS((short)5), 
	CONSOLE((short)6);
	
	private short type;
	
	DataOriginType(short type){
		this.type = type;
	}
	
	public short ofType(){
		return type;
	}
	
}
