package uk.ac.imperial.lsds.seepworker.core.input;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Dataset;

public class DatasetInputAdapter implements InputAdapter {

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	
	private int streamId;
	private Dataset dataset;
	private ITuple iTuple;
	
	private ByteBuffer header = ByteBuffer.allocate(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	private ByteBuffer payload = null;
	private int nTuples = 0;
	
	private Deque<byte[]> iTupleBuffer;
	
	public DatasetInputAdapter(WorkerConfig wc, int streamId, Dataset dataset, Schema expectedSchema) {
		this.streamId = streamId;
		this.dataset = dataset;
		this.iTuple = new ITuple(expectedSchema);
		this.iTupleBuffer = new ArrayDeque<>();
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
		// In this case it's dynamic.
		return dataset.getDataReference().getDataStore().type();
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		
		if(iTupleBuffer.size() == 0) {
		
			// FIXME: This buffer is pointing to the last written position. THINK this
			ByteBuffer sourceData = dataset.buffer;
			// FIXME: temporal solution?
			sourceData.flip();
			// TODO: write a more convenient method, if possible readablebytechannel to 
			// so basically create a readablebytechannel from sourceData
			
			// Read first into a stream from the dataset (should be indep of where dataset keeps )
			if(header.remaining() > 0) {
				this.read(sourceData, header);
			}
			
			if(payload == null && !header.hasRemaining()) {
				header.flip();
				byte control = header.get();
				nTuples = header.getInt();
				int payloadSize = header.getInt(); // payload size
				payload = ByteBuffer.allocate(payloadSize);
			}
			
			if(payload != null) {
				this.read(sourceData, payload);
				if(!payload.hasRemaining()) {
					
					int tupleSize = 0;
					payload.flip(); // Prepare buffer to read
					for(int i = 0; i < nTuples; i++){			
						tupleSize = payload.getInt();
						byte[] completedRead = new byte[tupleSize];
						payload.get(completedRead, 0, tupleSize);
						
						iTupleBuffer.push(completedRead);
					}
					payload.clear();
					payload = null;
					header.clear();
					nTuples = 0;
				}
			}
		}
		byte[] data = iTupleBuffer.pop();
		iTuple.setData(data);
		iTuple.setStreamId(streamId);
		return iTuple;
	}
	
	private int read(ByteBuffer src, ByteBuffer dst) {
		int offset = src.position();
		int length = dst.remaining();
		dst.put(src.array(), offset, length);
		src.position(offset + length); // advance position after reading from backup array
		return 0;
	}

	@Override
	public List<ITuple> pullDataItems(int timeout) {
		// TODO: yeah
		return null;
	}

}
