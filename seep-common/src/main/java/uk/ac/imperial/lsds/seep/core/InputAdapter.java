package uk.ac.imperial.lsds.seep.core;

import java.nio.channels.ReadableByteChannel;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.DataItem;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;

public interface InputAdapter {

	public List<Integer> getRepresentedOpId();
	public int getStreamId();
	public short returnType();
	
	public DataStoreType getDataOriginType();
	
	public void readFrom(ReadableByteChannel channel, int id);
	
	public void pushData(byte[] data);
	public void pushData(List<byte[]> data);
	
	public DataItem pullDataItem(int timeout);
	public DataItem pullDataItems(int timeout);
	
}
