package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import java.net.InetAddress;

import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.DataEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;
import uk.ac.imperial.lsds.seep.util.Utils;

public class PhysicalNode implements ExecutionUnit {

	private static final ExecutionUnitType executionUnitType = ExecutionUnitType.PHYSICAL_NODE;
	
	private ControlEndPoint cep;
	private DataEndPoint dep;
	private int id;

	public PhysicalNode(InetAddress ip, int controlPort, int dataPort) {
		this.id = Utils.computeIdFromIpAndPort(ip, controlPort);
		this.dep = new DataEndPoint(id, ip.getHostAddress(), dataPort);
		this.cep = new ControlEndPoint(id, ip.getHostAddress(), controlPort);
	}

	
	@Override
	public DataEndPoint getDataEndPoint() {
		return dep;
	}
	
	@Override
	public ControlEndPoint getControlEndPoint() {
		return cep;
	}


	@Override
	public int getId() {
		return id;
	}

	@Override
	public ExecutionUnitType getType() {
		return executionUnitType;
	}
	
	@Override
	public String toString(){
		String ls = System.getProperty("line.separator");
		StringBuilder sb = new StringBuilder();
		sb.append("TYPE: " + executionUnitType.name());
		sb.append(ls);
		sb.append("IP: " + dep.getIp().toString());
		sb.append(ls);
		sb.append("Data-PORT: " + dep.getPort());
		sb.append(ls);
		sb.append("Control-PORT: " + cep.getPort());
		sb.append(ls);
		return sb.toString();
	}

}