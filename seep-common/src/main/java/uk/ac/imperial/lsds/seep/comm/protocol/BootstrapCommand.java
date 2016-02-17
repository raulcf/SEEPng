package uk.ac.imperial.lsds.seep.comm.protocol;

public class BootstrapCommand implements CommandType {

	private String ip;
	private int controlport;
	private int dataPort;
	
	public BootstrapCommand(){}
	
	public BootstrapCommand(String ip, int controlPort, int dataPort){
		this.ip = ip;
		this.controlport = controlPort;
		this.dataPort = dataPort;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.BOOTSTRAP.type();
	}
	
	public String getIp() {
		return ip;
	}
	
	public int getControlPort() {
		return controlport;
	}
	
	public int getDataPort() {
		return dataPort;
	}

}
