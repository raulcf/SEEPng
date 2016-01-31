package uk.ac.imperial.lsds.seep.comm.protocol;

public class MasterWorkerCommand implements SeepCommand {

	private short type;
	
	private BootstrapCommand bc;
	private CrashCommand cc;
	private CodeCommand coc;
//	private QueryDeployCommand qdc;
	private StartQueryCommand sqc;
	private StopQueryCommand stqc;
	private DeadWorkerCommand dwc;
	private ScheduleDeployCommand sdc;
	private ScheduleStageCommand esc;
	private StageStatusCommand ssc;
	private MaterializeTaskCommand mtc;
	private LocalSchedulerElectCommand lsec;
	private LocalSchedulerStagesCommand lssc;
	
	public MasterWorkerCommand(){}
	
	public MasterWorkerCommand(CommandType ct){
		short type = ct.type();
		this.type = type;
		if(type == MasterWorkerProtocolAPI.BOOTSTRAP.type()){
			this.bc = (BootstrapCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.CRASH.type()){
			this.cc = (CrashCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.CODE.type()){
			this.coc = (CodeCommand)ct;
		}
//		else if(type == MasterWorkerProtocolAPI.QUERYDEPLOY.type()){
//			this.qdc = (QueryDeployCommand)ct;
//		}
		else if(type == MasterWorkerProtocolAPI.STARTQUERY.type()){
			this.sqc = (StartQueryCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.STOPQUERY.type()){
			this.stqc = (StopQueryCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.DEADWORKER.type()){
			this.dwc = (DeadWorkerCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.SCHEDULE_TASKS.type()){
			this.sdc = (ScheduleDeployCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.SCHEDULE_STAGE.type()) {
			this.esc = (ScheduleStageCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.LOCAL_ELECT.type()){
			this.lsec = (LocalSchedulerElectCommand)ct;
		}
		else if (type == MasterWorkerProtocolAPI.LOCAL_SCHEDULE.type()){
			this.lssc = (LocalSchedulerStagesCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.STAGE_STATUS.type()) {
			this.ssc = (StageStatusCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.MATERIALIZE_TASK.type()) {
			this.mtc = (MaterializeTaskCommand)ct;
		}
		else{
			try {
				throw new Exception("NOT DEFINED CLASS HERE !!!");
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("ERROR: "+e.getMessage());
			}
		}
	}
	
	public short type(){
		return type;
	}
	
	public BootstrapCommand getBootstrapCommand(){
		return bc;
	}
	
	public CrashCommand getCrashCommand(){
		return cc;
	}
	
	public CodeCommand getCodeCommand(){
		return coc;
	}
	
//	public QueryDeployCommand getQueryDeployCommand(){
//		return qdc;
//	}
	
	public StartQueryCommand getStartQueryCommand(){
		return sqc;
	}
	
	public StopQueryCommand getStopQueryCommand(){
		return stqc;
	}
	
	public DeadWorkerCommand getDeadWorkerCommand(){
		return dwc;
	}
	
	public ScheduleDeployCommand getScheduleDeployCommand(){
		return sdc;
	}
	
	public ScheduleStageCommand getScheduleStageCommand() {
		return esc;
	}
	
	public LocalSchedulerElectCommand getLocalSchedulerElectCommand(){
		return lsec;
	}
	
	public LocalSchedulerStagesCommand getLocalSchedulerStageCommand(){
		return lssc;
	}
	
	public StageStatusCommand getStageStatusCommand() {
		return ssc;
	}
	
	public MaterializeTaskCommand getMaterializeTaskCommand() {
		return mtc;
	}
}
