package uk.ac.imperial.lsds.seep.comm.protocol;

import uk.ac.imperial.lsds.seep.api.SeepPhysicalQuery;

public class QueryDeployCommand implements CommandType {

	private SeepPhysicalQuery psq;
	
	public QueryDeployCommand(){}
	
	public QueryDeployCommand(SeepPhysicalQuery psq){
		this.psq = psq;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.QUERYDEPLOY.type();
	}
	
	public SeepPhysicalQuery getQuery(){
		return psq;
	}

}
