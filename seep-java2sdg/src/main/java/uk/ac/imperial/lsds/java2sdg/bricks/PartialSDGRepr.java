package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElement;
import uk.ac.imperial.lsds.seep.api.DataStore;

public class PartialSDGRepr {

	// First TE will have ID 1 - Source will be 0
	private static int currentId;
	
	private String workflowName;
	private DataStore source;
	private DataStore sink;
	private List<TaskElement> te = new ArrayList<TaskElement>();
	

	public static PartialSDGRepr makePartialSDGRepr(String workflowName, DataStore source,
			List<PartialSDGComponent> pSDGComponents, DataStore sink) {
		currentId++;
		return new PartialSDGRepr(currentId, workflowName, source, pSDGComponents, sink);
	}

	private PartialSDGRepr(int currentId, String workflowName, DataStore source,
			List<PartialSDGComponent> pSDGComponents, DataStore sink) {
		// store source and sink as attributes of this class and use them later with the code generation module
		this.workflowName = workflowName;
		this.source = source;
		this.sink = sink;

		// TODO: Create connection from source to the first TE in the pSDGComponents list
		for (PartialSDGComponent p : pSDGComponents) {
			TaskElement te = new TaskElement(currentId, p.te, p.inputVariables, p.outputVariables);
			this.te.add(te);
		}
		// Create connections between these TE in the order of the list (the
		// order they were split) - Downstream
		for (int i = 0; i < (this.te.size() - 1); i++)
			this.te.get(i).getDownstreams().add(this.te.get(i + 1).getId());

		// TODO: create last connection between the last TE and the sink
	}

	public static int getCurrentPartialSDGReprId() {
		return currentId;
	}
	
	public String getWorkflowName() {
		return workflowName;
	}

	public void setWorkflowName(String workflowName) {
		this.workflowName = workflowName;
	}	

	public List<TaskElement> getTEs() {
		return te;
	}

	public DataStore getSource() {
		return source;
	}

	public DataStore getSink() {
		return sink;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nTaskElement: " + te.toString());
		return sb.toString();
	}

}
