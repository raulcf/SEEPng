package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.java2sdg.bricks.VariableRepr;
import uk.ac.imperial.lsds.seep.api.data.Schema;

public class TaskElement {

	private final int id;
	private List<Integer> downstreams; // id of downstreams
	private List<Integer> upstreams;   // id of upstreams
	
	private List<VariableRepr> inputVariables;
	private List<String> code;
	private List<VariableRepr> outputVariables;
	private Schema outputSchema;
	
	private boolean isSouce = false;
	private boolean isSink = false;
	
	
	public TaskElement(int id){
		this.id = id;
		this.downstreams = new ArrayList<Integer>();
		this.upstreams = new ArrayList<Integer>();
		this.inputVariables = new ArrayList<VariableRepr>();
		this.code = new ArrayList<String>();
		this.outputVariables = new ArrayList<VariableRepr>();
	}

	public TaskElement(int id, TaskElement ter,
			List<VariableRepr> inputVariables,
			List<VariableRepr> outputVariables) {
		this.id = id;
		this.inputVariables = inputVariables;
		this.outputVariables = outputVariables;
		this.code = ter.code;
		this.downstreams = ter.downstreams;
		this.upstreams = ter.upstreams;
		this.outputSchema = ter.outputSchema;
		
	}
	
	public int getId() {
		return id;
	}
	
	public List<Integer> getDownstreams() {
		return downstreams;
	}

	public void setDownstreams(List<Integer> downstreams) {
		this.downstreams = downstreams;
	}

	public List<Integer> getUpstreams() {
		return upstreams;
	}

	public void setUpstreams(List<Integer> upstreams) {
		this.upstreams = upstreams;
	}

	public List<VariableRepr> getInputVariables() {
		return inputVariables;
	}

	public void setInputVariables(List<VariableRepr> initialVariables) {
		this.inputVariables = initialVariables;
	}

	public List<String> getCode() {
		return code;
	}

	public void setCode(List<String> code) {
		this.code = code;
	}

	public List<VariableRepr> getOutputVariables() {
		return outputVariables;
	}

	public void setOutputVariables(List<VariableRepr> outputVariables) {
		this.outputVariables = outputVariables;
	}

	public Schema getOutputSchema() {
		return outputSchema;
	}

	public void setOutputSchema(Schema outputSchema) {
		this.outputSchema = outputSchema;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("TaskElemRepr ID: "+ id + "\n");
		sb.append("\t\t  DownStreams: "+ downstreams+ "\n");
		sb.append("\t\t  Upstreams: " + upstreams+ "\n");
		String tmp = "";
		for(VariableRepr t : inputVariables)
			tmp +=t.getName()+", ";
		sb.append("\t\t  InputVariables: "+ tmp + "\n");
		sb.append("\t\t  Code: "+ code+ "\n");
		sb.append("\t\t  OutputVariables: ");
		for(VariableRepr v : this.outputVariables)
			sb.append(v.getName()+", ");
		sb.append("\n" );
		sb.append("\t\t  OutputSchema: "+ outputSchema+ "\n");
		return sb.toString();
	}

	public boolean isSouce() {
		return isSouce;
	}

	public boolean isSink() {
		return isSink;
	}

	public void setSouce(boolean isSouce) {
		this.isSouce = isSouce;
	}

	public void setSink(boolean isSink) {
		this.isSink = isSink;
	}

}
