package ir;

import java.util.List;

public interface Traceable {
	
	public enum TraceableType {
		TASK, DATA;
	};
	
	public void composeIdGenerator(IdGen idGen);
	public int getId();
	public void setName(String name);
	public TraceableType getTraceableType();
	public String getName();
	public void addInput(Traceable t);
	public void addOutput(Traceable t);
	public void isInputOf(Traceable t);
	public void isOutputOf(Traceable t);
	public List<Traceable> getOutput();
	public String toString();
	
}
