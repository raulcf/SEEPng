package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;

public interface ExecutionUnit {
	
	public ExecutionUnitType getType();
	public int getId();
	public String toString();
	public SeepEndPoint getDataEndPoint();
	public SeepEndPoint getMasterControlEndPoint();
	public SeepEndPoint getWorkerControlEndPoint();
	
}
