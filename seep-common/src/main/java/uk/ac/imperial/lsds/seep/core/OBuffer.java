package uk.ac.imperial.lsds.seep.core;

import java.nio.channels.WritableByteChannel;

import uk.ac.imperial.lsds.seep.api.DataReference;

public interface OBuffer {

	public void setEventAPI(EventAPI eAPI);
	public EventAPI getEventAPI();
	
	public int id();
	public DataReference getDataReference();
	public boolean drainTo(WritableByteChannel channel);
	public boolean write(byte[] data);
	public boolean ready();
	
}
