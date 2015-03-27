package uk.ac.imperial.lsds.seep.comm.protocol;

public enum MasterWorkerProtocolAPI {
	BOOTSTRAP((short)0, new BootstrapCommand()), 
	CRASH((short)1, new CrashCommand()), 
	CODE((short)2, new CodeCommand()), 
	QUERYDEPLOY((short)3, new QueryDeployCommand()),
	STARTQUERY((short)5, new StartQueryCommand()),
	STOPQUERY((short)6, new StopQueryCommand()),
	DEADWORKER((short)7, new DeadWorkerCommand());
	
	private short type;
	private CommandType c;
	
	MasterWorkerProtocolAPI(short type, CommandType c){
		this.type = type;
		this.c = c;
	}
	
	public short type(){
		return type;
	}
	
	public CommandType clazz(){
		return c;
	}
}
