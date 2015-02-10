package uk.ac.imperial.lsds.java2sdg.bricks;

import uk.ac.imperial.lsds.seep.api.DataOrigin;
import uk.ac.imperial.lsds.seep.util.Utils;

public class WorkflowRepr {

	// Workflow name. this is what we were storing previously
	private String name;
	// data origin type, told by the annotation, file, network, etc...
	private DataOrigin source;
	// whether it has a sink annotation, and in that case, what type. will need to become a enum in the future
	private DataOrigin sink;
	
	public WorkflowRepr(String name, DataOrigin source, DataOrigin sink){
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

	public DataOrigin getSource() {
		return source;
	}

	public void setSource(DataOrigin source) {
		this.source = source;
	}

	public DataOrigin getSink() {
		return sink;
	}

	public void setSink(DataOrigin sink) {
		this.sink = sink;
	}
	
	public boolean hasSink(){
		return this.sink != null;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("Workflow: "+name);
		sb.append(Utils.NL);
		sb.append("Source: ");
		sb.append(Utils.NL);
		sb.append(source.toString());
		sb.append(Utils.NL);
		if(sink != null){
			sb.append("Sink: ");
			sb.append(Utils.NL);
			sb.append(sink.toString());
		}
		return sb.toString();
	}
	
}
