package uk.ac.imperial.lsds.seep.core;

import java.nio.channels.WritableByteChannel;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEventRegister;
import uk.ac.imperial.lsds.seep.api.data.OTuple;

public interface OBuffer {

	public int id();
	public DataReference getDataReference();
	public boolean drainTo(WritableByteChannel channel);
	public boolean write(byte[] data, RuntimeEventRegister reg);
	public boolean write(OTuple o, RuntimeEventRegister reg);
	public boolean readyToWrite();
	public void flush();
}
