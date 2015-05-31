package uk.ac.imperial.lsds.seepworker.comm;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.protocol.RequestDataReferenceCommand;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;

public class WorkerWorkerAPIImplementation {

	private DataReferenceManager drm;
	
	public WorkerWorkerAPIImplementation(Comm comm, Conductor c, DataReferenceManager drm, WorkerConfig wc){
		this.drm = drm;
	}

	public void handleRequestDataReferenceCommand(RequestDataReferenceCommand requestDataReferenceCommand) {
		int dataRefId = requestDataReferenceCommand.getDataReferenceId();
		int rxPort = requestDataReferenceCommand.getReceivingDataPort();
		
		// FIXME: create outgoingConnection and give it to network selector to start serving stuff
//		
//		// Make sure DRM manages this DataReferenceId
//		DataReference dr = drm.doesManageDataReference(dataRefId);
//		if (dr == null){
//			// FIXME: error
//			System.exit(-1);
//		}
	}
	
}
