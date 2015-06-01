package uk.ac.imperial.lsds.seepworker.comm;

import java.net.InetAddress;

import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.protocol.RequestDataReferenceCommand;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;

public class WorkerWorkerAPIImplementation {

	private Conductor c;
	
	public WorkerWorkerAPIImplementation(Comm comm, Conductor c, WorkerConfig wc){
		this.c = c;
	}

	public void handleRequestDataReferenceCommand(RequestDataReferenceCommand requestDataReferenceCommand) {
		int dataRefId = requestDataReferenceCommand.getDataReferenceId();
		InetAddress ip = requestDataReferenceCommand.getIp();
		int rxPort = requestDataReferenceCommand.getReceivingDataPort();
		
		// Create target endPoint
		int id = Utils.computeIdFromIpAndPort(ip, rxPort);
		EndPoint ep = new EndPoint(id, ip, rxPort);
		
		c.serveData(dataRefId, ep);
	}
	
}
