package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.List;

import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElement;

public class PartialSDGComponent {

	public TaskElement te;
	public List<VariableRepr> inputVariables;
	public List<VariableRepr> outputVariables;
	
	public PartialSDGComponent(TaskElement te, List<VariableRepr> inputVariables, List<VariableRepr> outputVariables) {
		this.te = te;
		this.inputVariables = inputVariables;
		this.outputVariables = outputVariables;
	}

	/**
	 * @return the TaskElementRepr
	 */
	public TaskElement getTe() {
		return te;
	}

	/**
	 * @return the inputVariables
	 */
	public List<VariableRepr> getInputVariables() {
		return inputVariables;
	}

	/**
	 * @return the outputVariables
	 */
	public List<VariableRepr> getOutputVariables() {
		return outputVariables;
	}
}
