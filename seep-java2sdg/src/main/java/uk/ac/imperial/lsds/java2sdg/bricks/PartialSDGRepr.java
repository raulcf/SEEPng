package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.List;

import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElementRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.VariableRepr;

public class PartialSDGRepr {

	private static int currentId;
	private List<TaskElementRepr> te;
	
	public static PartialSDGRepr makePartialSDGRepr(String workflowName, TaskElementRepr ter, List<VariableRepr> inputVariables,
			List<VariableRepr> outputVariables) {
		currentId++;
		return new PartialSDGRepr(currentId, workflowName, ter, inputVariables, outputVariables);
	}
	
	private PartialSDGRepr(int currentId, String workflowName, TaskElementRepr ter,
			List<VariableRepr> inputVariables,
			List<VariableRepr> outputVariables) {
		TaskElementRepr te = new TaskElementRepr(currentId, ter, inputVariables, outputVariables);
		this.te.add(te);
	}
	
	public int getPartialSDGReprId() {
		return currentId;
	}
	
	public List<TaskElementRepr> getTEs() {
		return te;
	}

	
}
