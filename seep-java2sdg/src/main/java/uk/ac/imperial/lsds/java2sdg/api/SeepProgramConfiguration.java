package uk.ac.imperial.lsds.java2sdg.api;

import java.util.HashMap;
import java.util.Map;

import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.errors.InvalidQueryDefinitionException;

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
	 * @param name The name of the workflow
	 * @param inputDataStore The input data store
	 */
	public void newWorkflow(String name, DataStore inputDataStore){
		newWorkflow(name, inputDataStore, null);
	}
	
	/**
	 * API to declare a new workflow that has a sink
	 * @param name The name of the workflow
	 * @param inputDataStore The input data store
	 * @param outputDataStore The output data store
	 */
	public void newWorkflow(String name, DataStore inputDataStore, DataStore outputDataStore){
		if(workflows.containsKey(name))
			throw new InvalidQueryDefinitionException("Workflow with same name already registered");
		
		WorkflowRepr wr = new WorkflowRepr(name, inputDataStore, outputDataStore);
		this.workflows.put(name, wr);
	}
	
}