package uk.ac.imperial.lsds.seep.core;

public interface EventBasedOBuffer extends OBuffer {

	public void setEventAPI(EventAPI eAPI);
	public EventAPI getEventAPI();
	
}
