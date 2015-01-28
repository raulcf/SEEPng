package uk.ac.imperial.lsds.seep.comm.protocol;

import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;

public class QueryDeployCommand implements CommandType {

	private PhysicalSeepQuery psq;
	
	public QueryDeployCommand(){}
	
	public QueryDeployCommand(PhysicalSeepQuery psq){
		this.psq = psq;
	}
	
	@Override
	public short type() {
		return MasterWorkerProtocolAPI.QUERYDEPLOY.type();
	}
	
	public PhysicalSeepQuery getQuery(){
		return psq;
	}

}
