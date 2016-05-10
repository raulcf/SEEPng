package ir;

public interface Traceable {
	
	public int getId();
	public void setName(String name);
	public String getName();
	public void addInput(int id);
	public void addOutput(int id);
	
}
