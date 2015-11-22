package uk.ac.imperial.lsds.seepworker.core;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;

public class Dataset implements IBuffer, OBuffer {

	private int id;
	private DataReference dataReference;
	
	
	public ByteBuffer buffer = ByteBuffer.allocate(8192);
	private final int BATCH_SIZE = 9; // FIXME: rethink this, too much overhead per tuple
	
	// Writing artifacts
	private AtomicBoolean completed = new AtomicBoolean(false);
	private int tuplesInBatch = 0;
	private int currentBatchSize = 0;

	public Dataset(DataReference dataReference) {
		this.dataReference = dataReference;
		this.id = dataReference.getId();
		allocateInitialBuffer();
	}
	
	public Dataset(int id, byte[] syntheticData, DataReference dr) {
		this.dataReference = dr;
		this.id = id;
		// FIXME: for now, just make sure nobody writes and reads this simultaneously
		this.buffer = ByteBuffer.allocate(8192);
		// This data is ready to be simply copied over
		buffer.put(syntheticData);
	}
	
	private void allocateInitialBuffer() {
		// FIXME: for now, just make sure nobody writes and reads this simultaneously
		this.buffer = ByteBuffer.allocate(8192);
		this.buffer.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	}
	
	public Schema getSchemaForDataset() {
		return this.dataReference.getDataStore().getSchema();
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
			//buffer.flip(); // leave the buffer ready to be read FIXME: not in store mode....
		}
		return true;
	}

	@Override
	public boolean readyToWrite() {
		// TODO Auto-generated method stub
		return false;
	}
	
}
