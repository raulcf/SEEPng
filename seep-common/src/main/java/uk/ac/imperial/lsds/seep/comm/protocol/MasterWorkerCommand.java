package uk.ac.imperial.lsds.seep.comm.protocol;

public class MasterWorkerCommand {

	private short type;
	
	private BootstrapCommand bc;
	private CrashCommand cc;
	private CodeCommand coc;
	private QueryDeployCommand qdc;
	private StartQueryCommand sqc;
	private StopQueryCommand stqc;
	private DeadWorkerCommand dwc;
	
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
		else if(type == MasterWorkerProtocolAPI.QUERYDEPLOY.type()){
			this.qdc = (QueryDeployCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.STARTQUERY.type()){
			this.sqc = (StartQueryCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.STOPQUERY.type()){
			this.stqc = (StopQueryCommand)ct;
		}
		else if(type == MasterWorkerProtocolAPI.DEADWORKER.type()){
			this.dwc = (DeadWorkerCommand)ct;
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
	
	public QueryDeployCommand getQueryDeployCommand(){
		return qdc;
	}
	
	public StartQueryCommand getStartQueryCommand(){
		return sqc;
	}
	
	public StopQueryCommand getStopQueryCommand(){
		return stqc;
	}
	
	public DeadWorkerCommand getDeadWorkerCommand(){
		return dwc;
	}
}
