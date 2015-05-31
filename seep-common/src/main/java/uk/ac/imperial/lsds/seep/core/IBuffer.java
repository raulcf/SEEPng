package uk.ac.imperial.lsds.seep.core;

import java.nio.channels.ReadableByteChannel;

public interface IBuffer {

	public void readFrom(ReadableByteChannel channel);
	public byte[] read(int timeout);
	void pushData(byte[] data);
	
}
