package uk.ac.imperial.lsds.java2sdg.bricks;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.data.Schema;


/**
 * WorkflowRepr is an internal representation of a workflow in SEEP.
 * @author ra
 */
public class WorkflowRepr {

	// Workflow name. The name of the workflow
	private String name;
	// The input data store, e.g. file, network, kafka, etc
	private DataStore source;
	// The schema of the input data
	private Schema inputSchema;
	// The output data store, e.g. file, network, kafka, console, etc
	private DataStore sink;
	// The schema of the output data
	private Schema outputSchema;
	
	public WorkflowRepr(String name, DataStore source, Schema inputSchema, DataStore sink, Schema outputSchema){
		this.name = name;
		this.source = source;
		this.inputSchema = inputSchema;
		this.sink = sink;
		this.outputSchema = outputSchema;
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
	
	public Schema getInputSchema(){
		return inputSchema;
	}
	
	public void setInputSchema(Schema inputSchema){
		this.inputSchema = inputSchema;
	}

	public DataStore getSink() {
		return sink;
	}

	public void setSink(DataStore sink) {
		this.sink = sink;
	}
	
	public Schema getOutputSchema(){
		return outputSchema;
	}
	
	public void setOutputSchema(Schema outputSchema){
		this.outputSchema = outputSchema;
	}
	
	public boolean hasSink(){
		return this.sink != null;
	}
}
