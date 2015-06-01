package uk.ac.imperial.lsds.seep.comm.protocol;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
	
	public InetAddress getIp() {
		try {
			return InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public int getReceivingDataPort() {
		return receivingDataPort;
	}

}
