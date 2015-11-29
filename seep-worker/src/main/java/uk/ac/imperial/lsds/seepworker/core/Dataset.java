package uk.ac.imperial.lsds.seepworker.core;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;

public class Dataset implements IBuffer, OBuffer {

	private int id;
	private DataReference dataReference;
	private BufferPool bufferPool;
	
	private Queue<ByteBuffer> buffers;
	private Iterator<ByteBuffer> readerIterator;
	private ByteBuffer wPtrToBuffer;
	private ByteBuffer rPtrToBuffer;

	public Dataset(DataReference dataReference, BufferPool bufferPool) {
		this.dataReference = dataReference;
		this.id = dataReference.getId();
		this.bufferPool = bufferPool;
		this.wPtrToBuffer = bufferPool.borrowBuffer();
		this.wPtrToBuffer.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
		this.buffers = new LinkedList<>();
		this.buffers.add(wPtrToBuffer);
	}
	
	public Dataset(int id, byte[] syntheticData, DataReference dr) {
		this.dataReference = dr;
		this.id = id;
		// FIXME: for now, just make sure nobody writes and reads this simultaneously
		this.wPtrToBuffer = bufferPool.borrowBuffer();
		// This data is ready to be simply copied over
		wPtrToBuffer.put(syntheticData);
		this.buffers = new LinkedList<>();
		this.buffers.add(wPtrToBuffer);
	}
	
	public List<byte[]> consumeData(int numTuples) {
		// TODO: Implement
		return null;
	}
	
	public byte[] consumeData() {
		// Lazily initialize Iterator
		if(readerIterator == null) {
			readerIterator = this.buffers.iterator();
		}
		
		// Get next buffer for reading
		if(rPtrToBuffer == null || rPtrToBuffer.remaining() == 0) {
			// When the buffer is read completely we return it to the pool
			if(rPtrToBuffer != null) {
				bufferPool.returnBuffer(rPtrToBuffer);
			}
			if(readerIterator.hasNext()) {
				rPtrToBuffer = readerIterator.next();
				rPtrToBuffer.flip();
			}
			else {
				// done reading
				return null;
			}
		}
		
		int size = rPtrToBuffer.getInt();
		byte[] data = new byte[size];
		rPtrToBuffer.get(data);
		
		return data;
	}
	
	public Schema getSchemaForDataset() {
		return this.dataReference.getDataStore().getSchema();
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
		return dataReference;
	}

	@Override
	public boolean drainTo(WritableByteChannel channel) {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public boolean write(byte[] data) {
		
		int dataSize = data.length;
		if(wPtrToBuffer.remaining() < dataSize + TupleInfo.TUPLE_SIZE_OVERHEAD) {
			// Borrow a new buffer and add to the collection
			this.wPtrToBuffer = bufferPool.borrowBuffer();
			this.buffers.add(wPtrToBuffer);
		}
		
		wPtrToBuffer.putInt(dataSize);
		wPtrToBuffer.put(data);
		
		return true;
	}
	
	/**
	 * IBuffer interface
	 */
	
	@Override
	public boolean readyToWrite() {
		// TODO Auto-generated method stub
		return false;
	}

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
	
}
