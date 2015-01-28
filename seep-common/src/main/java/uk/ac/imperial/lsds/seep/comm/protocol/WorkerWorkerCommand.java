package uk.ac.imperial.lsds.seep.comm.protocol;

public class WorkerWorkerCommand {

	private short type;
	
	private AckCommand ac;
	private CrashCommand cc;
	
	public WorkerWorkerCommand(){}
	
	public WorkerWorkerCommand(CommandType ct){
		short type = ct.type();
		this.type = type;
		if(type == WorkerWorkerProtocolAPI.ACK.type()){
			this.ac = (AckCommand)ct;
		}
		else if(type == WorkerWorkerProtocolAPI.CRASH.type()){
			this.cc = (CrashCommand)ct;
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
	
	public AckCommand getAckCommand(){
		return ac;
	}
	
	public CrashCommand getCrashCommand(){
		return cc;
	}
}

