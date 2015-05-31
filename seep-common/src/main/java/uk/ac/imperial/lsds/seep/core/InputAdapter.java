package uk.ac.imperial.lsds.seep.core;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;

public interface InputAdapter {

	public int getStreamId();
	public short returnType();
	public DataStoreType getDataStoreType();
	
	public ITuple pullDataItem(int timeout);
	public List<ITuple> pullDataItems(int timeout);
	
}
