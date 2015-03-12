package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.Schema;

public class TaskElementRepr {

	private final int id;
	private List<Integer> downstreams; // id of downstreams
	private List<Integer> upstreams;   // id of upstreams
	
	private List<VariableRepr> initialVariables;
	private List<String> code;
	private List<VariableRepr> outputVariables;
	private Schema outputSchema;
	
	public TaskElementRepr(int id){
		this.id = id;
	}

}
