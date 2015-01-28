package uk.ac.imperial.lsds.seep.comm.protocol;

public enum WorkerWorkerProtocolAPI {
	ACK((short)0, new AckCommand()),
	CRASH((short)1, new CrashCommand());
	
	private short type;
	private CommandType ct;
	
	WorkerWorkerProtocolAPI(short type, CommandType ct){
		this.type = type;
		this.ct = ct;
	}
	
	public short type(){
		return type;
	}
	
	public CommandType clazz(){
		return ct;
	}
}
