package uk.ac.imperial.lsds.seep.comm.protocol;

public class DeadWorkerCommand implements CommandType {
	
	private String reason = "Not indicated";
	
	public DeadWorkerCommand(){}
	
	public DeadWorkerCommand(String reason){
		this.reason = reason;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.DEADWORKER.type();
	}
	
	public String reason(){
		return reason;
	}

}
