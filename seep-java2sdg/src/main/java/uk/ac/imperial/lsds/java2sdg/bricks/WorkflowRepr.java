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
	
	
	public WorkflowRepr(String name, DataStore source, DataStore sink){
		this.name = name;
		this.source = source;
		this.sink = sink;
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

	public DataStore getSink() {
		return sink;
	}

	public void setSink(DataStore sink) {
		this.sink = sink;
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
		sb.append("\nSource: \n" + this.source.getSchema());
		sb.append("--------------");
		sb.append("\nSink: \n"+ this.sink.getSchema());
		sb.append("--------------");
		sb.append("\nCode: "+ this.code);
		return sb.toString();
		
	}
}
