package uk.ac.imperial.lsds.seep.comm.protocol;

public class RequestDataReferenceCommand implements CommandType {

	private int dataReferenceId;
	private int receivingDataPort;
	
	public RequestDataReferenceCommand() { }
	
	public RequestDataReferenceCommand(int dataReferenceId, int receivingDataPort) {
		this.dataReferenceId = dataReferenceId;
		this.receivingDataPort = receivingDataPort;
	}

	@Override
	public short type() {
		return WorkerWorkerProtocolAPI.REQUEST_DATAREF.type();
	}
	
	public int getDataReferenceId() {
		return dataReferenceId;
	}
	
	public int getReceivingDataPort() {
		return receivingDataPort;
	}

}
