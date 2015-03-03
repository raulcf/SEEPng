package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStore;

public class WorkflowRepr {

	// Workflow name. this is what we were storing previously
	private String name;
	// data origin type, told by the annotation, file, network, etc...
	private DataStore source;
	// whether it has a sink annotation, and in that case, what type. will need to become a enum in the future
	private DataStore sink;
	
	public WorkflowRepr(String name, DataStore source, DataStore sink){
		this.name = name;
		this.source = source;
		this.sink = sink;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DataStore getSource() {
		return source;
	}

	public void setSource(DataStore source) {
		this.source = source;
	}

	public DataStore getSink() {
		return sink;
	}

	public void setSink(DataStore sink) {
		this.sink = sink;
	}
	
	public boolean hasSink(){
		return this.sink != null;
	}
	
	public WorkflowRepr(String name, DataStore dOrigin, boolean hasSink, List<Object> inputParameters){
		// TODO: perhaps we want to pass other information here and transform it to the above attributes
	}
	
}
