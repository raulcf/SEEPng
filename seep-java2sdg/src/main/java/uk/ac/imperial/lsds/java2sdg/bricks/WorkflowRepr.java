package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStore;

public class WorkflowRepr {

	// Workflow name. this is what we were storing previously
	private String name;
	// data origin type, told by the annotation, file, network, etc...
	private DataStore dOrigin;
	// whether it has a sink annotation, and in that case, what type. will need to become a enum in the future
	private boolean hasSink;
	/**
	 *  list of input parameters. we will get the schema from input parameters.
	 *  we can get this list once we have the soot method
	 */
	private List<Object> inputParameters;
	
	public WorkflowRepr(String name, DataStore dOrigin, boolean hasSink, List<Object> inputParameters){
		// TODO: perhaps we want to pass other information here and transform it to the above attributes
	}
	
}
