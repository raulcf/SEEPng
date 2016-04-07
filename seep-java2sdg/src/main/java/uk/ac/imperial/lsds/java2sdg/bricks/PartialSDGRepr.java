package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElementRepr;
import uk.ac.imperial.lsds.seep.api.DataStore;

public class PartialSDGRepr {

	private static int currentId;
	private List<TaskElementRepr> te = new ArrayList<TaskElementRepr>();
	
	public static PartialSDGRepr makePartialSDGRepr(String workflowName, 
			DataStore source, 
			List<PartialSDGComponents> pSDGComponents, 
			DataStore sink) {
		currentId++;
		return new PartialSDGRepr(currentId, workflowName, source, pSDGComponents, sink);
	}
	
	private PartialSDGRepr(int currentId, String workflowName, 
			DataStore source, 
			List<PartialSDGComponents> pSDGComponents, 
			DataStore sink) {
		// Option 1: create a TaskElementRepr for source and sink and connect with the others.
		// Option 2: store source and sink as attributes of this class and use them later with the code generation module
		// TODO: create connection from source to the first TE in the pSDGComponents list 
		for(PartialSDGComponents p : pSDGComponents) {
			// TODO: create connections between these TE in the order of the list (the order they were split)
			TaskElementRepr te = new TaskElementRepr(currentId, p.te, p.inputVariables, p.outputVariables);
			this.te.add(te);
		}
		// TODO: create last connection between the last TE and the sink
	}
	
	public int getPartialSDGReprId() {
		return currentId;
	}
	
	public List<TaskElementRepr> getTEs() {
		return te;
	}
	
}
