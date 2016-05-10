package ir;

import java.util.List;

public class Dummy implements Traceable {

	final private int id;
	private List<Integer> inputs;
	private List<Integer> outputs;
	private String name;
	
	public Dummy(int id) {
		this.id = id;
	}
	
	public void setName(String s) {
		this.name = s;
	}
	
	public String getName() {
		return name;
	}
	
	public int getId() {
		return id;
	}
	
	public void addInput(int id) {
		inputs.add(id);
	}
	
	public void addOutput(int id) {
		outputs.add(id);
	}
}
