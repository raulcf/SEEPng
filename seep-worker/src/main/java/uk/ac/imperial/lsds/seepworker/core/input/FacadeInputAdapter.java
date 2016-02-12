package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;

public class FacadeInputAdapter implements InputAdapter{

	private int streamId;
	
	final private short RETURN_TYPE;
	
	public FacadeInputAdapter(int streamId, InputAdapterReturnType returnType) {
		this.streamId = streamId;
		this.RETURN_TYPE = returnType.ofType();
	}
	
	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public short returnType() {
		return RETURN_TYPE;
	}

	@Override
	public DataStoreType getDataStoreType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		// FIXME: temporal method
		ITuple it = new ITuple(null);
		return it;
	}

	@Override
	public List<ITuple> pullDataItems(int timeout) {
		// FIXME: temporal method
		List<ITuple> l = new ArrayList<>();
		ITuple it = new ITuple(null);
		l.add(it);
		return l;
	}

}
