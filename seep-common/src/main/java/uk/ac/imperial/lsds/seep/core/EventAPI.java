package uk.ac.imperial.lsds.seep.core;

import java.util.List;

public interface EventAPI {

	public void readyForWrite(int id);
	public void readyForWrite(List<Integer> ids);
	
}
