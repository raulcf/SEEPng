package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Map;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public class MaterializeTaskCommand implements CommandType {

	private Map<Integer, EndPoint> mapping;
	
	public MaterializeTaskCommand() { }
	
	public MaterializeTaskCommand(Map<Integer, EndPoint> mapping) { 
		this.mapping = mapping;
	}

	@Override
	public short type() {
		return MasterWorkerProtocolAPI.MATERIALIZE_TASK.type();
	}
	
	public Map<Integer, EndPoint> getMapping() {
		return mapping;
	}
	
}
