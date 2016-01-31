package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Set;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public class LocalSchedulerElectCommand implements CommandType {

	private Set<EndPoint> workerNodes;

	public LocalSchedulerElectCommand() {}

	public LocalSchedulerElectCommand(Set<EndPoint> workers) {
		this.workerNodes = workers;
	}

	@Override
	public short type() {
		return MasterWorkerProtocolAPI.LOCAL_ELECT.type();
	}

	/**
	 * @return the workerNodes
	 */
	public Set<EndPoint> getWorkerNodes() {
		return workerNodes;
	}
	

}
