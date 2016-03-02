package uk.ac.imperial.lsds.seep.api;

import java.util.List;

public interface RuntimeEventRegister {

	public List<RuntimeEvent> getRuntimeEvents();
	public void exception(String message);
	public void datasetSpilledToDisk(int datasetId);
	public void failure();
	
}
