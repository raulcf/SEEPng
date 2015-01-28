package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.CommAPI;
import uk.ac.imperial.lsds.seepworker.comm.EventAPI;

public interface OutputAdapter extends CommAPI{
	
	public int getStreamId();
	public Map<Integer, OutputBuffer> getOutputBuffers();
	public void setEventAPI(EventAPI eAPI);
	
	public boolean requiresNetwork();
	public boolean requiresFile();
	
}

