package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import java.net.InetAddress;

import uk.ac.imperial.lsds.seep.infrastructure.DataEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;
import uk.ac.imperial.lsds.seep.infrastructure.MasterControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.WorkerControlEndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;

public class PhysicalNode implements ExecutionUnit {

	private static final ExecutionUnitType executionUnitType = ExecutionUnitType.PHYSICAL_NODE;
	
	private DataEndPoint dep;
	private MasterControlEndPoint mcep;
	private WorkerControlEndPoint wcep;
	private int id;

	public PhysicalNode(InetAddress ip, int masterControlPort, int dataPort, int workerControlPort) {
		this.id = Utils.computeIdFromIpAndPort(ip, masterControlPort);
		this.dep = new DataEndPoint(id, ip.getHostAddress(), dataPort);
		this.mcep = new MasterControlEndPoint(id, ip.getHostAddress(), masterControlPort);
		this.wcep = new WorkerControlEndPoint(id, ip.getHostAddress(), workerControlPort);
	}

	
	@Override
	public SeepEndPoint getDataEndPoint() {
		return dep;
	}

	@Override
	public SeepEndPoint getWorkerControlEndPoint() {
		return wcep;
	}
	
	@Override
	public SeepEndPoint getMasterControlEndPoint() {
		return mcep;
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
		sb.append("Control-PORT: " + wcep.getPort());
		sb.append(ls);
		return sb.toString();
	}

}