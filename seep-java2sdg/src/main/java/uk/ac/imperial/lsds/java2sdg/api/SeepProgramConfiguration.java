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
	 * @param inputSchema The schema of the input data
	 */
	public void newWorkflow(String name, DataStore inputDataStore, Schema inputSchema){
		newWorkflow(name, inputDataStore, inputSchema, null, null);
	}
	
	/**
	 * API to declare a new workflow that has a sink
	 * @param name The name of the workflow
	 * @param inputDataStore The input data store
	 * @param inputSchema The schema of the input data
	 * @param outputDataStore The output data store
	 * @param outputSchema The schema of the output data
	 */
	public void newWorkflow(String name, DataStore inputDataStore, Schema inputSchema, DataStore outputDataStore, Schema outputSchema){
		if(workflows.containsKey(name))
			throw new InvalidQueryDefinitionException("Workflow with same name already registered");
		
		WorkflowRepr wr = new WorkflowRepr(name, inputDataStore, inputSchema, outputDataStore, outputSchema);
		this.workflows.put(name, wr);
	}
	
}