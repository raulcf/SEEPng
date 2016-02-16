package uk.ac.imperial.lsds.seep.comm.protocol;

public class BootstrapCommand implements CommandType {

	private String ip;
	private int masterControlport;
	private int dataPort;
	private int workerControlPort;
	
	public BootstrapCommand(){}
	
	public BootstrapCommand(String ip, int port, int dataPort, int controlPort){
		this.ip = ip;
		this.masterControlport = port;
		this.dataPort = dataPort;
		this.workerControlPort = controlPort;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.BOOTSTRAP.type();
	}
	
	public String getIp() {
		return ip;
	}
	
	public int getMasterControlPort() {
		return masterControlport;
	}
	
	public int getDataPort() {
		return dataPort;
	}
	
	public int getWorkerControlPort() {
		return workerControlPort;
	}

}
