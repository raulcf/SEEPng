package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;

public class FacadeInputAdapter implements InputAdapter {

	private int streamId;
	private DataStoreType dst;
	
	private boolean doneInvocation = false;
	
	final private short RETURN_TYPE;
	
	public FacadeInputAdapter(int streamId, InputAdapterReturnType returnType, IBuffer buffer) {
		this.streamId = streamId;
		this.RETURN_TYPE = returnType.ofType();
		this.dst = buffer.getDataReference().getDataStore().type();
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
		return dst;
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		// When there are no input parameters we invoke the task once
		if(!doneInvocation) {
			doneInvocation = true;
			ITuple it = ITuple.makeEmptyITuple();
			return it;
		}
		return null; // Done execution
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
