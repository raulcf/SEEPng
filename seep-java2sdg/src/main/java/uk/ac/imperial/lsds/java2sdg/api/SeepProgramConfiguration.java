package uk.ac.imperial.lsds.java2sdg.api;

import java.util.HashMap;
import java.util.Map;

import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.InvalidInitializationException;

public class SeepProgramConfiguration {
	
	private Map<String, WorkflowRepr> workflows;
	
	public SeepProgramConfiguration(){
		workflows = new HashMap<>();
	}
	
	public Map<String, WorkflowRepr> getWorkflows(){
		return workflows;
	}
	
	/**
	 * API to declare a new workflow that does not have a sink
	 * @param name
	 * @param idc
	 */
	public void newWorkflow(String name, DataStore input){
		newWorkflow(name, input, null);
	}
	
	/**
	 * API to declare a new workflow that has a sink
	 * @param name
	 * @param idc
	 * @param odc
	 */
	public void newWorkflow(String name, DataStore input, DataStore output){
		if(workflows.containsKey(name))
			throw new InvalidInitializationException("Workflow with same name already registered");
		
		WorkflowRepr wr = new WorkflowRepr(name, input, output);
		this.workflows.put(name, wr);
	}
	
}