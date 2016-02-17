package uk.ac.imperial.lsds.seep.comm.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.comm.serialization.MapWrapper;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;

public class MaterializeTaskCommand implements CommandType {
	
	private Map<Integer, ControlEndPoint> mapping;
	private Map<Integer, MapWrapper> inputs;
	private Map<Integer, MapWrapper> outputs;
	
	public MaterializeTaskCommand() { }
	
	public MaterializeTaskCommand(
			Map<Integer, ControlEndPoint> mapping, 
			Map<Integer, Map<Integer, Set<DataReference>>> inputs, 
			Map<Integer, Map<Integer, Set<DataReference>>> outputs) { 
		this.mapping = mapping;
		
		this.inputs = new HashMap<>();
		this.outputs = new HashMap<>();
		
		// Adapt to inner representation for serialisation/deserialisation
		for(Integer key : inputs.keySet()) {
			MapWrapper mw = new MapWrapper();
			mw.el = inputs.get(key);
			this.inputs.put(key, mw);
		}
		for(Integer key : outputs.keySet()) {
			MapWrapper mw = new MapWrapper();
			mw.el = outputs.get(key);
			this.outputs.put(key, mw);
		}
	}

	@Override
	public short type() {
		return MasterWorkerProtocolAPI.MATERIALIZE_TASK.type();
	}
	
	public Map<Integer, Map<Integer, Set<DataReference>>> getInputs() {
		Map<Integer, Map<Integer, Set<DataReference>>> rInputs = new HashMap<>();
		for(Integer key : inputs.keySet()) {
			rInputs.put(key, inputs.get(key).el);
		}
		return rInputs;
	}
	
	public Map<Integer, Map<Integer, Set<DataReference>>> getOutputs() {
		Map<Integer, Map<Integer, Set<DataReference>>> rOutputs = new HashMap<>();
		for(Integer key : outputs.keySet()) {
			rOutputs.put(key, outputs.get(key).el);
		}
		return rOutputs;
	}
	
	public Map<Integer, ControlEndPoint> getMapping() {
		return mapping;
	}
	
}
