package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.ArrayList;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public class LocalSchedulerElectCommand implements CommandType {

	private ArrayList<EndPoint> workerNodes;

	public LocalSchedulerElectCommand() {}

	public LocalSchedulerElectCommand(ArrayList<EndPoint> workers) {
		this.workerNodes = workers;
	}

	@Override
	public short type() {
		return MasterWorkerProtocolAPI.LOCAL_ELECT.type();
	}

	/**
	 * @return the workerNodes
	 */
	public ArrayList<EndPoint> getWorkerNodes() {
		return workerNodes;
	}

}
