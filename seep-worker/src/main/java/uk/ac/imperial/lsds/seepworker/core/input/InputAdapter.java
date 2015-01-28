package uk.ac.imperial.lsds.seepworker.core.input;

import java.nio.channels.ReadableByteChannel;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.ITuple;


public interface InputAdapter {

	public List<Integer> getRepresentedOpId();
	public int getStreamId();
	public short returnType();
	
	public boolean requiresNetwork();
	public boolean requiresFile();
	
	public void readFrom(ReadableByteChannel channel, int id);
	
	public void pushData(byte[] data);
	public void pushData(List<byte[]> data);
	
	public ITuple pullDataItem(int timeout);
	public ITuple pullDataItems(int timeout);
	
}
