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
	// The output data store, e.g. file, network, kafka, console, etc
	private DataStore sink;
	// Actual code of workflow
	private CodeRepr code;
	
	// The schema of the input data
	@Deprecated // already included in source DataStore
	private Schema inputSchema;
	// The schema of the output data
	@Deprecated // already included in sink DataStore
	private Schema outputSchema;
	
	public WorkflowRepr(String name, DataStore source, Schema inputSchema, DataStore sink, Schema outputSchema){
		this.name = name;
		this.source = source;
		this.sink = sink;
		this.inputSchema = inputSchema;
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
	
	public void setCode(CodeRepr code){
		this.code = code;
	}
	
	public CodeRepr getCode(){
		return code;
	}
	@Override
	public String toString(){
		// Synchronization is not a concern here
		StringBuilder  sb = new StringBuilder();
		sb.append("\nName: "+ this.name);
		sb.append("\nSource: "+ this.source);
		sb.append("\nSink: "+ this.sink);
		sb.append("\nCode: "+ this.code);
		sb.append("\nDeprecated: \nIN: "+ this.inputSchema + "\nOUT: "+this.outputSchema);
		return sb.toString();
		
	}
}
