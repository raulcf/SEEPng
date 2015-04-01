package uk.ac.imperial.lsds.seep.infrastructure;


public interface ExecutionUnit {
	
	public ExecutionUnitType getType();
	public int getId();
	public String toString();
	public EndPoint getEndPoint();
	
}
