package uk.ac.imperial.lsds.seepworker.comm;

import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Conductor;
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;

public class WorkerWorkerAPIImplementation {

	private DataReferenceManager drm;
	
	public WorkerWorkerAPIImplementation(Comm comm, Conductor c, DataReferenceManager drm, WorkerConfig wc){
		this.drm = drm;
	}
	
}
