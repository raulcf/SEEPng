package uk.ac.imperial.lsds.seep.api;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.ITuple;

public interface SeepTask {
	
	public void setUp();
	public void processData(ITuple data, API api); // consider moving this to external interfaces
	public void processDataGroup(List<ITuple> d, API api); // also change tuple with tagging ifaces
	public void close();
	
}
