package uk.ac.imperial.lsds.seep.comm.protocol;


public class RequestDataReferenceCommand implements CommandType {

	private int dataReferenceId;
	private String ip;
	private int receivingDataPort;
	
	public RequestDataReferenceCommand() { }
	
	public RequestDataReferenceCommand(int dataReferenceId, String ip, int receivingDataPort) {
		this.dataReferenceId = dataReferenceId;
		this.ip = ip;
		this.receivingDataPort = receivingDataPort;
	}

	@Override
	public short type() {
		return WorkerWorkerProtocolAPI.REQUEST_DATAREF.type();
	}
	
	public int getDataReferenceId() {
		return dataReferenceId;
	}
	
	public String getIp() {
		return ip;
	}
	
	public int getReceivingDataPort() {
		return receivingDataPort;
	}

}
