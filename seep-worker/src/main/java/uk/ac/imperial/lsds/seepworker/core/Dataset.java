package uk.ac.imperial.lsds.seepworker.core;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEventRegister;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;

// No thread safe. In particular no simultaneous write and read is allowed right now
public class Dataset implements IBuffer, OBuffer {

	private int id;
	private DataReferenceManager drm;
	private DataReference dataReference;
	private BufferPool bufferPool;
	
	private Queue<ByteBuffer> buffers;
	private Iterator<ByteBuffer> readerIterator;
	private ByteBuffer wPtrToBuffer;
	private ByteBuffer rPtrToBuffer;
	
	// FIXME: this guy should not have this info. Instead, put this along with the 
	// dataset in a helper class, and do the management outside this. Open issue for this.
	private String cacheFileName = "";

	public Dataset(DataReference dataReference, BufferPool bufferPool, DataReferenceManager drm) {
		this.drm = drm;
		this.dataReference = dataReference;
		this.id = dataReference.getId();
		this.bufferPool = bufferPool;
		this.wPtrToBuffer = bufferPool.borrowBuffer();
		assert(this.wPtrToBuffer != null); // enough memory available for the initial buffer
		this.buffers = new LinkedList<>();
		this.buffers.add(wPtrToBuffer);
	}
	
	public Dataset(int id, byte[] syntheticData, DataReference dr, BufferPool bufferPool) {
		// This method does not need the DataReferenceManager as it's only used for producing data
		// one cannot write to it
		this.dataReference = dr;
		this.id = id;
		this.bufferPool = bufferPool;
		this.wPtrToBuffer = bufferPool.borrowBuffer();
		assert(this.wPtrToBuffer != null); // enough memory available for the initial buffer
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
				readerIterator.remove();
				bufferPool.returnBuffer(rPtrToBuffer);
			}
			if(readerIterator.hasNext()) {
				rPtrToBuffer = readerIterator.next();
				rPtrToBuffer.flip();
				
				if (!readerIterator.hasNext()) {
					//We caught up to the write buffer. Allocate a new buffer for writing
					this.wPtrToBuffer = bufferPool.borrowBuffer();
					this.buffers.add(wPtrToBuffer);
					//Yes, the following looks a bit silly (just getting a new iterator to the position
					//of the current one), but it is necessary to allow readerIterator.remove to work 
					//without the iterator complaining about concurrent modification due to adding a new
					//write buffer to the list.
					readerIterator = this.buffers.iterator();
					rPtrToBuffer = readerIterator.next();
					if (rPtrToBuffer.remaining() == 0) {
						return null;
					}
				}
			}
			else {
				// done reading
				return null;
			}
		}

		
		// FIXME: This is written to handle the case of having empty dataset
		// howver, that case should be handled in a more principled way, and before
		if(! rPtrToBuffer.hasRemaining()) {
			return null;
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
	public boolean write(byte[] data, RuntimeEventRegister reg) {
		
		// Check whether we have memory space to write data
		// if not try to borrow buffer, if this fails, spill to disk
		int dataSize = data.length;
		if(wPtrToBuffer.remaining() < dataSize + TupleInfo.TUPLE_SIZE_OVERHEAD) {
			// Borrow a new buffer and add to the collection
			this.wPtrToBuffer = bufferPool.borrowBuffer();
			if(this.wPtrToBuffer == null) {
				// Notify DRM we run out of memory and get ids of spilled to disk datasets
				List<Integer> spilledDatasets = drm.spillDatasetsToDisk(id);
				for(int spilledDatasetId : spilledDatasets) {
					reg.datasetSpilledToDisk(spilledDatasetId);
				}
			}
			else {
				this.buffers.add(wPtrToBuffer);
			}
		}
		
		// Check if dataset was spilled to disk, in which case we need to write there
		if (!cacheFileName.equals("")) {
			//If this dataset has been cached to disk write the data there instead of using up memory
			try {
				DataOutputStream cacheStream = new DataOutputStream(new FileOutputStream(cacheFileName, true));
				cacheStream.writeInt(data.length);
				cacheStream.write(data);
				cacheStream.flush();
				cacheStream.close();
				return true;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException ioe) {
				// TODO Auto-generated catch block
				ioe.printStackTrace();
			}
			return false;
		}

		// If dataset is living in memory we write it directly
		wPtrToBuffer.putInt(dataSize);
		wPtrToBuffer.put(data);
		return true;
	}
	
	public void setCachedLocation(String filename) {
		cacheFileName = filename;
	}
	
	public void unsetCachedLocation() {
		cacheFileName = "";
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
	public int readFrom(ReadableByteChannel channel) {
		// TODO Auto-generated method stub
		return -1;
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

	@Override
	public void flush() {
		// TODO Auto-generated method stub
		
	}

}
