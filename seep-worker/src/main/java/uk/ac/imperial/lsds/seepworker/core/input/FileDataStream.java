package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class FileDataStream implements InputAdapter {

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	final private DataStoreType TYPE = DataStoreType.FILE;
	
	final private int streamId;
	private ITuple iTuple;
	
	private IBuffer buffer;
	
	public FileDataStream(WorkerConfig wc, int streamId, IBuffer buffer, Schema expectedSchema){
		this.streamId = streamId;
		this.iTuple = new ITuple(expectedSchema);
		this.buffer = buffer;
	}
	
	public static FileDataStream getFileDataStream_test(int streamId, IBuffer buffer, Schema s){
		return new FileDataStream(null, streamId, buffer, s);
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
		return TYPE;
	}

	@Override
	public List<ITuple> pullDataItems(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		byte[] data = buffer.read(timeout);
		if(data == null) {
			return null;
		}
		iTuple.setData(data);
		iTuple.setStreamId(streamId);
		return iTuple;
	}

}
