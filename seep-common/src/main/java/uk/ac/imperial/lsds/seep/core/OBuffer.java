package uk.ac.imperial.lsds.seep.core;

import java.nio.channels.WritableByteChannel;

import uk.ac.imperial.lsds.seep.api.DataReference;

public interface OBuffer {

	public int id();
	public DataReference getDataReference();
	public boolean drainTo(WritableByteChannel channel);
	public boolean write(byte[] data);
	public boolean readyToWrite();
	public void flush();	
}
