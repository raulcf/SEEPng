package ir;

import java.util.ArrayList;
import java.util.List;

public class TraceSeed implements Traceable {

	private int id;
	private List<Traceable> inputs;
	private List<Traceable> outputs;
	private String name;
	
	public TraceSeed(int id) {
		this.id = id;
		this.inputs = new ArrayList<>();
		this.outputs = new ArrayList<>();
	}
	
	@Override
	public int getId() {
		return id;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void addInput(Traceable t) {
		inputs.add(t);
	}

	@Override
	public void addOutput(Traceable t) {
		outputs.add(t);
	}

	@Override
	public void isInputOf(Traceable t) {
		t.addInput(this);
	}

	@Override
	public void isOutputOf(Traceable t) {
		t.addOutput(this);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("ID: " + id);
		sb.append(System.lineSeparator());
		sb.append("Name: " + name);
		sb.append(System.lineSeparator());
		
		sb.append("Inputs: " + inputs.size());
		sb.append(System.lineSeparator());
		for(int i = 0; i < inputs.size(); i++) {
			sb.append(inputs.get(i));
			sb.append(System.lineSeparator());
		}
		
		sb.append("Outputs: " + outputs.size());
		sb.append(System.lineSeparator());
		for(int i = 0; i < outputs.size(); i++) {
			sb.append(outputs.get(i));
			sb.append(System.lineSeparator());
		}
		
		return sb.toString();
	}

}
