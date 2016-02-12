package uk.ac.imperial.lsds.seep.core;

import java.nio.channels.ReadableByteChannel;

import uk.ac.imperial.lsds.seep.api.DataReference;

// TODO: probably transform this into a tagging interface, and compose it with the methods above for those
// iBuffer implementation that need it, e.g. ReadableIBuffer.
public interface IBuffer {

	public DataReference getDataReference();
	public int readFrom(ReadableByteChannel channel);
	public byte[] read(int timeout);
	void pushData(byte[] data);
	
}
