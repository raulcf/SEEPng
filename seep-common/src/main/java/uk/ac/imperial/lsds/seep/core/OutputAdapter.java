package uk.ac.imperial.lsds.seep.core;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.CommAPI;
import uk.ac.imperial.lsds.seep.api.DataStoreType;

public interface OutputAdapter extends CommAPI{
	
	public int getStreamId();
	public Map<Integer, OutputBuffer> getOutputBuffers();
	public void setEventAPI(EventAPI eAPI);
	
	public DataStoreType getDataOriginType();
	
}

