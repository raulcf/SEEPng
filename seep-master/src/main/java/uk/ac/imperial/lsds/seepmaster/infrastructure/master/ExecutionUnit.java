package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.DataEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;

public interface ExecutionUnit {
	
	public ExecutionUnitType getType();
	public int getId();
	public String toString();
	public DataEndPoint getDataEndPoint();
	public ControlEndPoint getControlEndPoint();
	
}
