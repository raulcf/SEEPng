package ir;

import java.util.List;

public class TraceSeed implements Traceable {

	private int id;
	private List<Traceable> inputs;
	private List<Traceable> outputs;
	private String name;
	
	public TraceSeed(int id) {
		this.id = id;
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

}
