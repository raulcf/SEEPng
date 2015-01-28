package uk.ac.imperial.lsds.seepworker.comm;

import java.util.List;

public interface EventAPI {

	public void readyForWrite(int id);
	public void readyForWrite(List<Integer> ids);
	
}
