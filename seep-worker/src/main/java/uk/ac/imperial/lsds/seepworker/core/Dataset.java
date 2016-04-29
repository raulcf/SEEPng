package uk.ac.imperial.lsds.seepworker.core;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
	
	// Variables to estimate cost of creating the dataset
	final private long creationTime;
	private long lastAccessForWriteTime;
	private long totalDataWrittenToThisDataset;
	
	// FIXME: this guy should not have this info. Instead, put this along with the 
	// dataset in a helper class, and do the management outside this. Open issue for this.
	private String cacheFileName = "";
	private long cacheFilePosition = 0;

	public Dataset(DataReference dataReference, BufferPool bufferPool, DataReferenceManager drm) {
		this.drm = drm;
		this.dataReference = dataReference;
		this.id = dataReference.getId();
		this.bufferPool = bufferPool;
		this.wPtrToBuffer = obtainInitialNewWPtrBuffer();
		assert(this.wPtrToBuffer != null); // enough memory available for the initial buffer
		this.buffers = new ConcurrentLinkedQueue<>();
		//this.buffers.add(wPtrToBuffer);
		this.addBufferToBuffers(wPtrToBuffer);
		this.creationTime = System.nanoTime();
	}
	
	public Dataset(int id, byte[] syntheticData, DataReference dr, BufferPool bufferPool) {
		// This method does not need the DataReferenceManager as it's only used for producing data
		// one cannot write to it
		this.dataReference = dr;
		this.id = id;
		this.bufferPool = bufferPool;
		this.wPtrToBuffer = obtainInitialNewWPtrBuffer();
		assert(this.wPtrToBuffer != null); // enough memory available for the initial buffer
		// This data is ready to be simply copied over
		wPtrToBuffer.put(syntheticData);
		this.buffers = new ConcurrentLinkedQueue<>();
		//this.buffers.add(wPtrToBuffer);
		this.addBufferToBuffers(wPtrToBuffer);
		this.creationTime = System.nanoTime();
	}
	
	public void resetDataset() {
		if(wPtrToBuffer != null) {
			bufferPool.returnBuffer(wPtrToBuffer);
		}
		this.wPtrToBuffer = obtainInitialNewWPtrBuffer();
		this.buffers = new ConcurrentLinkedQueue<>();
		//this.buffers.add(wPtrToBuffer);
		this.addBufferToBuffers(wPtrToBuffer);
		readerIterator = null;
		rPtrToBuffer = null;
		totalDataWrittenToThisDataset = 0;
		cacheFilePosition = 0;
	}
	
	public long size() {
		return totalDataWrittenToThisDataset;
	}
	
	public long creationCost() {
		if(this.lastAccessForWriteTime > 0) {
			return (this.lastAccessForWriteTime - this.creationTime);
		}
		else {
			return 0; // because we don't want negative cost
		}
	}
	
	public List<byte[]> consumeData(int numTuples) {
		// TODO: Implement
		return null;
	}
	
	public int freeDataset() {
		if(! cacheFileName.equals("")) {
			// we dont remove files
			return 0;
		}
		int totalFreedMemory = 0;
		synchronized(buffers) {
			for(ByteBuffer bb : buffers) {
				totalFreedMemory = totalFreedMemory + bufferPool.returnBuffer(bb);
			}
		}
		return totalFreedMemory;
	}
	
	private void addBufferToBuffers(ByteBuffer buf) {
		synchronized(buffers){
			if(buf != null) {
				buffers.add(buf);
			}
			else {
				System.out.println(buf);
			}
		}
	}
	
	private ByteBuffer obtainInitialNewWPtrBuffer() {
		ByteBuffer bb = bufferPool.borrowBuffer();
		while(bb == null) {
			// free some memory from the node: true/false
			// Notify DRM we run out of memory and get ids of spilled to disk datasets
			List<Integer> spilledDatasets = drm.spillDatasetsToDisk(id);
			if(spilledDatasets.isEmpty()) {
				// no more memory available, allocate buffer on disk
				
				// CREATE FILE ON DISK 
				String name = drm.createDatasetOnDisk(id);
				this.setCachedLocation(name);
				 
				
				break;
			}
			else {
				System.out.println("non empty");
				for(int a : spilledDatasets) {
					System.out.print(a + " ");
				}
			}
			// if true then try again
			bb = bufferPool.borrowBuffer();
		}
		return bb;
	}
	
	private ByteBuffer obtainNewWPtrBuffer() {
		
		ByteBuffer bb = bufferPool.borrowBuffer();
		while(bb == null) {
			// free some memory from the node: true/false
			// Notify DRM we run out of memory and get ids of spilled to disk datasets
			List<Integer> spilledDatasets = drm.spillDatasetsToDisk(id);
			if(spilledDatasets.isEmpty()) {
				System.out.println("test1");
				// no more memory available, allocate buffer on disk
				try {
					drm.sendDatasetToDisk(id);
				} 
				catch (IOException e) {
					e.printStackTrace();
				}
				break;
			}
			else {
				System.out.println("non empty");
				for(int a : spilledDatasets) {
					System.out.print(a + " ");
				}
			}
			// if true then try again
			bb = bufferPool.borrowBuffer();
		}
		return bb;
	}
	
	public synchronized byte[] consumeDataFromMemoryForCopy() {
		synchronized(buffers) {
		// Lazily initialize Iterator
		if(readerIterator == null) {
			readerIterator = this.buffers.iterator();
		}
		synchronized(readerIterator) {	
			// Get next buffer for reading
			if(rPtrToBuffer == null || rPtrToBuffer.remaining() == 0) {
				// When the buffer is read completely we return it to the pool
				if(rPtrToBuffer != null) {
					if(buffers.size() > 0) {
						readerIterator.remove(); 
						bufferPool.returnBuffer(rPtrToBuffer);
					}
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
		} // sync readerIterator
		} // sync buffers
		// FIXME: This is written to handle the case of having empty dataset
		// howver, that case should be handled in a more principled way, and before
		if(! rPtrToBuffer.hasRemaining()) {
			return null;
		}

		int size = rPtrToBuffer.getInt();
		byte[] data = new byte[size];
		rPtrToBuffer.get(data);
		if(data != null){
			if(data.length == 0) {
				System.out.println("data is 0");
			}
		}
		return data;
	}
	
	private byte[] consumeDataFromMemory() {
		synchronized(buffers) {
		// Lazily initialize Iterator
		if(readerIterator == null) {
			readerIterator = this.buffers.iterator();
		}
		} // sync buffers
		synchronized(readerIterator) {
		// Get next buffer for reading
		if(rPtrToBuffer == null || rPtrToBuffer.remaining() == 0) {
			// When the buffer is read completely we return it to the pool
			if(rPtrToBuffer != null) {
				readerIterator.remove();
				bufferPool.returnBuffer(rPtrToBuffer);
			}
			if(readerIterator.hasNext()) {
				rPtrToBuffer = readerIterator.next();
				if(rPtrToBuffer == null) {
					System.out.println("problem here");
				}
				rPtrToBuffer.flip();
			}	
				if (!readerIterator.hasNext()) {	
 					//We caught up to the write buffer. Allocate a new buffer for writing
 					//this.wPtrToBuffer = bufferPool._forceBorrowBuffer();
 					this.wPtrToBuffer = this.obtainNewWPtrBuffer();
 					//this.buffers.add(wPtrToBuffer);
 					this.addBufferToBuffers(wPtrToBuffer);
 					
 					
 					if(! buffers.isEmpty()) {
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
			}
			else {
				// done reading
				return null;
			}
		}

		// FIXME: This is written to handle the case of having empty dataset
		// however, that case should be handled in a more principled way, and before
		if(! rPtrToBuffer.hasRemaining()) {
			return null;
		}

		int size = rPtrToBuffer.getInt();
		if(size == 0) {
			return null; // done reading? FIXME: should not happen
		}
		byte[] data = new byte[size];
		rPtrToBuffer.get(data);
		if(data.length == 4) {
			System.out.println("");
		}
		return data;
	}
		
	private byte[] consumeDataFromDisk() {
		FileInputStream inputStream;
		try {
			inputStream = new FileInputStream(cacheFileName);
			inputStream.getChannel().position(cacheFilePosition);
			
			//It is used, in the while condition just below, but Eclipse's analyzer isn't 
			//smart enough to figure that out. This just saves us a warning.
			@SuppressWarnings("unused")
			int readSuccess;
			byte[] recordSizeBytes = new byte[Integer.BYTES];
			//If there is another record the next few bytes will be an int containing the size of said record.
			if ((readSuccess = inputStream.read(recordSizeBytes)) != -1) {
				//Convert the bytes giving us the size to an int and read exactly the next record
				int recordSize = ByteBuffer.wrap(recordSizeBytes).getInt();
				if(recordSize == 0) {
					inputStream.close();
					return null; // is this correct?
				}
				byte[] record = new byte[recordSize + Integer.BYTES];
				System.arraycopy(recordSizeBytes, 0, record,0, Integer.BYTES);
				inputStream.read(record, Integer.BYTES, recordSize);
				
				cacheFilePosition += Integer.BYTES + recordSize;
				inputStream.close();
				return record;
			}
			inputStream.close();
			return null;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
			
	public byte[] consumeData() {
		boolean readFromMem = false;
		
		
		byte[] data = null;
		if (cacheFileName.equals("")) {
			readFromMem = true;
			data = consumeDataFromMemory();
		}
		else {
			data = consumeDataFromDisk();
		}
		
		if(readFromMem && !cacheFileName.equals("")) {
			System.out.println("ERROR");
		}
		
		if(data != null){
			if(data.length == 0) {
				System.out.println("data is 0");
			}
		}
		
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
		totalDataWrittenToThisDataset = totalDataWrittenToThisDataset + data.length + TupleInfo.TUPLE_SIZE_OVERHEAD;
		this.lastAccessForWriteTime = System.nanoTime();
		
		// If the dataset is a file, then write to the file
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
		
		// if it is not a file (for now) then try to write to memory
		// Check whether we have memory space to write data
		// if not try to borrow buffer, if this fails, spill to disk
		int dataSize = data.length;
		if(wPtrToBuffer.remaining() < dataSize + TupleInfo.TUPLE_SIZE_OVERHEAD) {
			// Borrow a new buffer and add to the collection
			this.wPtrToBuffer = this.obtainNewWPtrBuffer();
			this.addBufferToBuffers(wPtrToBuffer);
		}
		
		
		// It could be that while trying to allocate more memory, this one is exhausted and the dataset is moved to disk
		// Just in case, double check, if that happened write to disk and if not .... ***
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
		else {
			// ...*** if not then write to memory
			// If dataset is living in memory we write it directly
			wPtrToBuffer.putInt(dataSize);
			wPtrToBuffer.put(data);
		}
		
		return true;
	}
	
	public void setCachedLocation(String filename) {
		cacheFileName = filename;
		cacheFilePosition = 0;
	}
	
	public void unsetCachedLocation() {
		cacheFileName = "";
	}
	
	public long cacheFileLocation() {
		return cacheFilePosition;
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
