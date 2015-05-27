package uk.ac.imperial.lsds.seep.api;


public enum DataStoreType {
	
	NETWORK((short)0, false),
	FILE((short)1, false),
	IPC((short)2, false), // ??
	RDD((short)3, false),
	KAFKA((short)4, true),
	HDFS((short)5, true),
	MEMORYMAPPED_BYTEBUFFER((short)6, false);
	
	private short type;
	private boolean external;
	
	DataStoreType(short type, boolean external) {
		this.type = type;
		this.external = external;
	}
	
	public boolean isExternal() {
		return external;
	}
	
	public short ofType() {
		return type;
	}
	
}
