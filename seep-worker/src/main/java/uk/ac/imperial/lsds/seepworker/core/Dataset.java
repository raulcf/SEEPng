package uk.ac.imperial.lsds.seepworker.core;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;

public class Dataset implements IBuffer, OBuffer {

	private int id;
	private DataReference dataReference;
	
	// FIXME: for now, just make sure nobody writes and reads this simultaneously
	public ByteBuffer buffer = ByteBuffer.allocate(8192);
	private final int BATCH_SIZE = 4096;
	
	// Writing artifacts
	private AtomicBoolean completed = new AtomicBoolean(false);
	private int tuplesInBatch = 0;
	private int currentBatchSize = 0;

	public Dataset(DataReference dataReference) {
		this.dataReference = dataReference;
		this.id = dataReference.getId();
	}
	
	public Dataset(int id, byte[] syntheticData) {
		this.dataReference = null;
		this.id = id;
		buffer.put(syntheticData);
	}
	
	
	/**
	 * IBuffer
	 */
	
	@Override
	public void readFrom(ReadableByteChannel channel) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] read(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pushData(byte[] data) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * OBuffer
	 */

	@Override
	public void setEventAPI(EventAPI eAPI) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public EventAPI getEventAPI() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int id() {
		return id;
	}

	@Override
	public DataReference getDataReference() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean drainTo(WritableByteChannel channel) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean write(byte[] data) {
		
		// FIXME: assume there is always enough space
		
		int tupleSize = data.length;
		buffer.putInt(tupleSize);
		buffer.put(data);
		tuplesInBatch++;
		currentBatchSize = currentBatchSize + tupleSize + TupleInfo.TUPLE_SIZE_OVERHEAD;
		
		if(buffer.position() >= BATCH_SIZE) {
			int currentPosition = buffer.position();
			int currentLimit = buffer.limit();
			buffer.position(TupleInfo.NUM_TUPLES_BATCH_OFFSET);
			buffer.putInt(tuplesInBatch);
			buffer.putInt(currentBatchSize);
			buffer.position(currentPosition);
			buffer.limit(currentLimit);
			buffer.flip(); // leave the buffer ready to be read
		}
		return true;
	}

	@Override
	public boolean readyToWrite() {
		// TODO Auto-generated method stub
		return false;
	}
	
}
