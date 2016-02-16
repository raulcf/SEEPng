package uk.ac.imperial.lsds.seep.comm.protocol;


public enum MasterWorkerProtocolAPI {
	
	BOOTSTRAP((short)0, new BootstrapCommand()), 
	CRASH((short)1, new CrashCommand()), 
	CODE((short)2, new CodeCommand()), 
	STARTQUERY((short)5, new StartQueryCommand()),
	STOPQUERY((short)6, new StopQueryCommand()),
	DEADWORKER((short)7, new DeadWorkerCommand()),
	SCHEDULE_TASKS((short)8, new ScheduleDeployCommand()),
	SCHEDULE_STAGE((short)9, new ScheduleStageCommand()),
	STAGE_STATUS((short)10, new StageStatusCommand()),
	MATERIALIZE_TASK((short)11, new MaterializeTaskCommand());
	// 3 is free
	
	private short type;
	private short familyType;
	private CommandType c;
	
	MasterWorkerProtocolAPI(short type, CommandType c){
		this.type = type;
		this.familyType = CommandFamilyType.MASTERCOMMAND.ofType();
		this.c = c;
	}
	
	public short type(){
		return type;
	}
	
	public short familyType() {
		return familyType;
	}
	
	public CommandType clazz(){
		return c;
	}

}
