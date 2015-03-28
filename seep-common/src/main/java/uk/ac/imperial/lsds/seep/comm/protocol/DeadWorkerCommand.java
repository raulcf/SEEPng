package uk.ac.imperial.lsds.seep.comm.protocol;

public class DeadWorkerCommand implements CommandType {
	
	private int workerId;
	private String reason = "Not indicated";
	
	public DeadWorkerCommand(){}
	
	public DeadWorkerCommand(int workerId, String reason){
		this.workerId = workerId;
		this.reason = reason;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.DEADWORKER.type();
	}
	
	public int getWorkerId() {
		return workerId;
	}
	
	public String reason(){
		return reason;
	}

}
