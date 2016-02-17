package uk.ac.imperial.lsds.seep.comm.protocol;

public class WorkerWorkerCommand implements SeepCommand {

	private short type;
	
	private AckCommand ac;
	private CrashCommand cc;
	private RequestDataReferenceCommand rdrc;
	
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
		else if(type == WorkerWorkerProtocolAPI.REQUEST_DATAREF.type()){
			this.rdrc = (RequestDataReferenceCommand)ct;
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
	
	@Override
	public short familyType() {
		return CommandFamilyType.WORKERCOMMAND.ofType();
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
	
	public RequestDataReferenceCommand getRequestDataReferenceCommand() {
		return rdrc;
	}

}

