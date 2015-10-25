package uk.ac.imperial.lsds.seepworker.comm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.protocol.RequestDataReferenceCommand;
import uk.ac.imperial.lsds.seep.infrastructure.DataEndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;

public class WorkerWorkerAPIImplementation {

	final private Logger LOG = LoggerFactory.getLogger(WorkerWorkerAPIImplementation.class.getName());
	
	private Conductor c;
	
	public WorkerWorkerAPIImplementation(Comm comm, Conductor c, WorkerConfig wc){
		this.c = c;
	}

	public void handleRequestDataReferenceCommand(RequestDataReferenceCommand requestDataReferenceCommand) {
		int dataRefId = requestDataReferenceCommand.getDataReferenceId();
		String ip = requestDataReferenceCommand.getIp();
		int rxPort = requestDataReferenceCommand.getReceivingDataPort();
		
		// Create target endPoint
		int id = Utils.computeIdFromIpAndPort(ip, rxPort);
		
		DataEndPoint dep = new DataEndPoint(id, ip, rxPort);
		LOG.info("Request to serve data to: {}", dep.toString());
		c.serveData(dataRefId, dep);
	}
	
}
