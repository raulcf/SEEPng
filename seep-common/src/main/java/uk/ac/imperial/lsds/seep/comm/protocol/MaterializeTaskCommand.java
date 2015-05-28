package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public class MaterializeTaskCommand implements CommandType {

	private Map<Integer, EndPoint> mapping;
	private Map<Integer, Map<Integer, Set<DataReference>>> inputs;
	private Map<Integer, Map<Integer, Set<DataReference>>> outputs;
		
	public MaterializeTaskCommand() { }
	
	public MaterializeTaskCommand(
			Map<Integer, EndPoint> mapping, 
			Map<Integer, Map<Integer, Set<DataReference>>> inputs, 
			Map<Integer, Map<Integer, Set<DataReference>>> outputs) { 
		this.mapping = mapping;
		this.inputs = inputs;
		this.outputs = outputs;
	}

	@Override
	public short type() {
		return MasterWorkerProtocolAPI.MATERIALIZE_TASK.type();
	}
	
	public Map<Integer, Map<Integer, Set<DataReference>>> getInputs() {
		return inputs;
	}
	
	public Map<Integer, Map<Integer, Set<DataReference>>> getOutputs() {
		return outputs;
	}
	
	public Map<Integer, EndPoint> getMapping() {
		return mapping;
	}
	
}
