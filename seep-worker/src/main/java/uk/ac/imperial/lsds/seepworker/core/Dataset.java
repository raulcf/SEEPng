package uk.ac.imperial.lsds.seepworker.core;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEventRegister;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.api.data.ZCITuple;
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
	
	private int diskAccess;
	private int memAccess;
	
	public static Dataset newDatasetOnDisk(DataReference dataRef,
			BufferPool bufferPool, DataReferenceManager drm) {
		return new Dataset(dataRef, bufferPool, drm, true);
	}
	
	public Dataset(DataReference dataReference, BufferPool bufferPool, DataReferenceManager drm, boolean onDisk) {
		this.drm = drm;
		this.dataReference = dataReference;
		this.id = dataReference.getId();
		this.bufferPool = bufferPool;
		
		// Get cache buffer, always one available
		this.wPtrToBuffer = this.obtainInitialNewWPtrBuffer();
		// If dataset is to be created on disk:
//		if(onDisk) {
//			String name = drm.createDatasetOnDisk(id);
//			this.setCachedLocation(name);
//		}
		this.buffers = new ConcurrentLinkedQueue<>();
		this.creationTime = System.nanoTime();
	}

	public Dataset(DataReference dataReference, BufferPool bufferPool, DataReferenceManager drm) {
		this.drm = drm;
		this.dataReference = dataReference;
		this.id = dataReference.getId();
		this.bufferPool = bufferPool;
		
		// Get cache buffer, always one available
		this.wPtrToBuffer = this.obtainInitialNewWPtrBuffer();
		this.buffers = new ConcurrentLinkedQueue<>();
		this.creationTime = System.nanoTime();
	}
	
	public Dataset(int id, DataReference dataReference, BufferPool bufferPool, DataReferenceManager drm) {
		this.id = id;
		this.drm = drm;
		this.dataReference = dataReference;
		this.bufferPool = bufferPool;
		
		// Get cache buffer, always one available
		this.wPtrToBuffer = this.obtainInitialNewWPtrBuffer();
		this.buffers = new ConcurrentLinkedQueue<>();
		this.creationTime = System.nanoTime();
	}
	
	public Dataset(int id, byte[] syntheticData, DataReference dr, BufferPool bufferPool) {
		// This method does not need the DataReferenceManager as it's only used for producing data
		// one cannot write to it
		this.dataReference = dr;
		this.id = id;
		this.bufferPool = bufferPool;
		
		// Get cache buffer, always one available
		this.wPtrToBuffer = this.obtainInitialNewWPtrBuffer();
		// This data is ready to be simply copied over
		wPtrToBuffer.put(syntheticData);
		this.buffers = new ConcurrentLinkedQueue<>();
		this.creationTime = System.nanoTime();
	}
	
	/**
	 * Call this method before moving a dataset to disk
	 * @return
	 */
	public Iterator<ByteBuffer> prepareForTransferToDisk() {
		if(!(this.buffers.size() > 0)) {
			System.out.println("NO DATA TO SPILL");
			System.exit(-1);
		}
		readerIterator = this.buffers.iterator();
		return readerIterator;
	}
	
	public ByteBuffer prepareForTransferToMemory() {
		ByteBuffer bb = this.wPtrToBuffer;
		this.buffers = new ConcurrentLinkedQueue<>();
		return bb;
	}
	
	/**
	 * Call this method after moving a dataset buffers to disk
	 * @return
	 */
	public int completeTransferToDisk() {
		if(!(this.buffers.size() > 0)) {
			System.out.println("NO DATA TO SPILL");
			System.exit(-1);
		}
		
		readerIterator = this.buffers.iterator();
		
		int freedMemory = 0;
		while(readerIterator.hasNext()) {
			ByteBuffer bb = readerIterator.next();
			freedMemory += bufferPool.returnBuffer(bb);
			readerIterator.remove();
		}
		return freedMemory;
	}
	
	public void transferToMemory(BufferedInputStream bis, int bbSize) {
		boolean goOn = true;
		while(goOn) {
			int limit = 0;
			ByteBuffer bb = null;
			try {
				limit = bis.read();
				if(limit == -1) {
					goOn = false;
					bis.close();
					continue;
				}
				bb = bufferPool.borrowBuffer();
				int read = 0;
				if(bb == null) { // Run out of memory, we can try with the cached buffer
					read = bis.read(wPtrToBuffer.array());
				}
				else {
					read = bis.read(bb.array());
				}
				if(read == -1) {
					goOn = false;
					bufferPool.returnBuffer(bb);
					bis.close();
					continue;
				}
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			// Add read buffer to memory
			if(bb == null) {
				wPtrToBuffer.limit(limit);
				wPtrToBuffer.flip();
				this.addBufferToBuffers(wPtrToBuffer);
			}
			else {
				bb.limit(limit);
				this.addBufferToBuffers(bb);
			}
		}
	}
	
	public void completeTransferToMemory(ByteBuffer currentPointer) {
		
		// Reset dataset so that it can be read again
		this.readerIterator = null;
		this.rPtrToBuffer = null;
		this.cacheFilePosition = 0;
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
	
	@Deprecated
	public void markAcess() {
		if (!cacheFileName.equals("")) {
			diskAccess++;
		}
		else {
			memAccess++;
		}
	}
	
	public int getDiskAccess() {
		return diskAccess;
	}
	
	public int getMemAccess() {
		return memAccess;
	}
	
	public int freeDataset() {
		int totalFreedMemory = 0;
		for(ByteBuffer bb : buffers) {
			totalFreedMemory = totalFreedMemory + bufferPool.returnBuffer(bb);
		}
		if(this.wPtrToBuffer != null) {
			totalFreedMemory = totalFreedMemory + bufferPool.returnBuffer(wPtrToBuffer);
		}
		
		return totalFreedMemory;
	}
	
	public void addBufferToBuffers(ByteBuffer buf) {
		if(buf != null) {
			buffers.add(buf);
		}
		else {
			System.out.println("ERROR ADDING EMPTY BUFFER TO MEMORY");
			System.exit(-1);
		}
	}
	
	private ByteBuffer obtainInitialNewWPtrBuffer() {
		
		ByteBuffer bb = bufferPool.borrowBuffer();
		if(bb == null) {
			String name = drm.createDatasetOnDisk(id);
			this.setCachedLocation(name);
			bb = bufferPool.getCacheBuffer();
		}
		return bb;
	}
	
	private ByteBuffer obtainNewWPtrBuffer() {
		
		ByteBuffer bb = bufferPool.borrowBuffer();
		if(bb == null) {
			try {
				drm.sendDatasetToDisk(id);
				bb = bufferPool.getCacheBuffer();
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		return bb;
	}
	
	
	public void prepareDatasetForFutureRead() {
		// For memory read only this is enough
		wPtrToBuffer = rPtrToBuffer; // Set wPtrToBuffer to that one
		rPtrToBuffer = null;
		readerIterator = this.buffers.iterator();
		// Flip all memory buffers
		while(readerIterator.hasNext()) {
			readerIterator.next().flip();
		}
		readerIterator = null; // Reset and let consumer create this again as needed
		// For file operations, reset
		cacheFilePosition = 0;
	}
	
	public void prepareSyntheticDatasetForRead() {
		rPtrToBuffer = null;
		readerIterator = null; // Reset and let consumer create this again as needed
		// For file operations, reset
		cacheFilePosition = 0;
	}
	
	public ITuple consumeData_zerocopy(ZCITuple t) {
		// Try to read from rPtrToBuffer
		if(rPtrToBuffer == null || rPtrToBuffer.remaining() == 0) {
			// MEMORY
			if (cacheFileName.equals("")) {
				memAccess++;
				if(readerIterator == null) {
					readerIterator = this.buffers.iterator();
				}
				if(readerIterator.hasNext()) {
					rPtrToBuffer = readerIterator.next();
					if(rPtrToBuffer.position() == rPtrToBuffer.limit()) {
						rPtrToBuffer.flip();
					}
				}
				else {
					// No more buffers available, read the write buffer
					if(wPtrToBuffer != null) {
						if(wPtrToBuffer.position() != 0) {
							wPtrToBuffer.flip();
						}
						rPtrToBuffer = wPtrToBuffer;
						if(rPtrToBuffer.limit() == 0) {
							System.out.println("B");
						}
						wPtrToBuffer = null;
					}
					else {
						prepareDatasetForFutureRead();
						return null;
					}
				}
			}
			// DISK
			else {
				diskAccess++;
				int minBufSize = bufferPool.getMinimumBufferSize();
				FileInputStream is = null;
				try {
					is = new FileInputStream(cacheFileName);
					is.getChannel().position(cacheFilePosition);
					byte[] d = new byte[minBufSize];
					int limit = is.read();
					if(limit == -1) {
						// if the write buffer still contains data
						if(wPtrToBuffer != null) {
							if(wPtrToBuffer.position() != 0) {
								wPtrToBuffer.flip();
							}
							rPtrToBuffer = wPtrToBuffer;
							if(rPtrToBuffer.limit() == 0) {
								System.out.println("D");
							}
							is.close();
							wPtrToBuffer = null;
						}
						else {
							is.close();
							prepareDatasetForFutureRead();
							return null;
						}
					}
					else {
						int read = is.read(d);
						if(read == -1) {
							// if the write buffer still contains data
							if(wPtrToBuffer != null) {
								if(wPtrToBuffer.position() != 0) {
									wPtrToBuffer.flip();
								}
								rPtrToBuffer = wPtrToBuffer;
								if(rPtrToBuffer.limit() == 0) {
									System.out.println("F");
								}
								is.close();
								wPtrToBuffer = null;
							}
							else {
								is.close();
								prepareDatasetForFutureRead();
								return null;
							}
						}
						else if (read != minBufSize) {
							System.out.println("Problem reading smaller buffer chunk (Dataset.consumeData)");
							System.exit(-1);
						}
						else {
							rPtrToBuffer = ByteBuffer.wrap(d);
							if(rPtrToBuffer.limit() == 0) {
								System.out.println("H");
							}
							cacheFilePosition += minBufSize + 1; // 4 limit size
							is.close();
						}
					} // else
				}
				catch (FileNotFoundException fnfe) {
					if(wPtrToBuffer != null) {
						if(wPtrToBuffer.position() != 0) {
							wPtrToBuffer.flip();
						}
						rPtrToBuffer = wPtrToBuffer;
						if(rPtrToBuffer.limit() == 0) {
							System.out.println("I");
						}
						// no need to close stream as it does not exist
						wPtrToBuffer = null;
					}
					else {
						// no need to close stream as it does not exist
						prepareDatasetForFutureRead();
						return null;
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			t.assignBuffer(rPtrToBuffer);
		}
		int size = rPtrToBuffer.getInt();
		int currentPosition = rPtrToBuffer.position();
		t.setBufferPtr(currentPosition);
		return t;
	}
			
	public byte[] consumeData() {
		// Try to read from rPtrToBuffer
		if(rPtrToBuffer == null || rPtrToBuffer.remaining() == 0) {
			// MEMORY
			if (cacheFileName.equals("")) {
				memAccess++;
				if(readerIterator == null) {
					readerIterator = this.buffers.iterator();
				}
				if(readerIterator.hasNext()) {
					rPtrToBuffer = readerIterator.next();
					if(rPtrToBuffer.position() == rPtrToBuffer.limit()) {
						rPtrToBuffer.flip();
					}
				}
				else {
					// No more buffers available, read the write buffer
					if(wPtrToBuffer != null) {
						if(wPtrToBuffer.position() != 0) {
							wPtrToBuffer.flip();
						}
						rPtrToBuffer = wPtrToBuffer;
						if(rPtrToBuffer.limit() == 0) {
							System.out.println("B");
						}
						wPtrToBuffer = null;
					}
					else {
						prepareDatasetForFutureRead();
						return null;
					}
				}
			}
			// DISK
			else {
				diskAccess++;
				int minBufSize = bufferPool.getMinimumBufferSize();
				FileInputStream is = null;
				try {
					is = new FileInputStream(cacheFileName);
					is.getChannel().position(cacheFilePosition);
					byte[] d = new byte[minBufSize];
					int limit = is.read();
					if(limit == -1) {
						// if the write buffer still contains data
						if(wPtrToBuffer != null) {
							if(wPtrToBuffer.position() != 0) {
								wPtrToBuffer.flip();
							}
							rPtrToBuffer = wPtrToBuffer;
							if(rPtrToBuffer.limit() == 0) {
								System.out.println("D");
							}
							is.close();
//							if(wPtrToBuffer.limit() == 0) return null;
							wPtrToBuffer = null;
						}
						else {
							is.close();
							prepareDatasetForFutureRead();
							return null;
						}
					}
					else {
						int read = is.read(d);
						if(read == -1) {
							// if the write buffer still contains data
							if(wPtrToBuffer != null) {
								if(wPtrToBuffer.position() != 0) {
									wPtrToBuffer.flip();
								}
								rPtrToBuffer = wPtrToBuffer;
								if(rPtrToBuffer.limit() == 0) {
									System.out.println("F");
								}
								is.close();
//								if(wPtrToBuffer.limit() == 0) return null;
								wPtrToBuffer = null;
							}
							else {
								is.close();
								prepareDatasetForFutureRead();
								return null;
							}
						}
						else if (read != minBufSize) {
							System.out.println("Problem reading smaller buffer chunk (Dataset.consumeData)");
							System.exit(-1);
						}
						else {
							rPtrToBuffer = ByteBuffer.wrap(d);
							if(rPtrToBuffer.limit() == 0) {
								System.out.println("H");
							}
							cacheFilePosition += minBufSize + 1; // 4 limit size
							is.close();
						}
					} // else
				}
				catch (FileNotFoundException fnfe) {
					if(wPtrToBuffer != null) {
						if(wPtrToBuffer.position() != 0) {
							wPtrToBuffer.flip();
						}
						rPtrToBuffer = wPtrToBuffer;
						if(rPtrToBuffer.limit() == 0) {
							System.out.println("I");
						}
						// no need to close stream as it does not exist
//						if(wPtrToBuffer.limit() == 0) return null;
						wPtrToBuffer = null;
					}
					else {
						// no need to close stream as it does not exist
						prepareDatasetForFutureRead();
						return null;
					}
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// At this point we have rPtrToBuffer
		int size = rPtrToBuffer.getInt();
		if(size == 0) {
			System.out.println();
		}
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
		int dataSize = data.length;
		totalDataWrittenToThisDataset = totalDataWrittenToThisDataset + dataSize + TupleInfo.TUPLE_SIZE_OVERHEAD;
		this.lastAccessForWriteTime = System.nanoTime();
		
		// Try to write to cache buffer first
		if(wPtrToBuffer.remaining() < dataSize + TupleInfo.TUPLE_SIZE_OVERHEAD) {
			// When buffer is full, then we check whether this dataset is in memory or not
			if (!cacheFileName.equals("")) { // disk
				transferBBToDisk();
				this.wPtrToBuffer = bufferPool.getCacheBuffer();
			}
			else { // memory
				wPtrToBuffer.flip();
				this.addBufferToBuffers(wPtrToBuffer); // add full buffer
				this.wPtrToBuffer = this.obtainNewWPtrBuffer(); // try to get a new one
			}
		}
		
		// Write size and data to cache buffer. Here it is guaranteed to exist
		wPtrToBuffer.putInt(dataSize);
		wPtrToBuffer.put(data);
		
		return true;
	}
	
	@Override
	public boolean write(OTuple o, RuntimeEventRegister reg) {
		int dataSize = o.getTupleSize();
		totalDataWrittenToThisDataset = totalDataWrittenToThisDataset + dataSize + TupleInfo.TUPLE_SIZE_OVERHEAD;
		this.lastAccessForWriteTime = System.nanoTime();
		
		// Try to write to cache buffer first
		if(wPtrToBuffer.remaining() < dataSize + TupleInfo.TUPLE_SIZE_OVERHEAD) {
			// When buffer is full, then we check whether this dataset is in memory or not
			if (!cacheFileName.equals("")) { // disk
				transferBBToDisk();
				this.wPtrToBuffer = bufferPool.getCacheBuffer();
			}
			else { // memory
				wPtrToBuffer.flip();
				this.addBufferToBuffers(wPtrToBuffer); // add full buffer
				this.wPtrToBuffer = this.obtainNewWPtrBuffer(); // try to get a new one
			}
		}
		
		// Write size and data to cache buffer. Here it is guaranteed to exist
		o.writeValues(wPtrToBuffer);
		
		return true;
	}
	
	private void transferBBToDisk() {
		BufferedOutputStream bos = null;
		try {
			// Open file to append buffer
			bos = new BufferedOutputStream(new FileOutputStream(cacheFileName, true), bufferPool.getMinimumBufferSize());
			int limit = wPtrToBuffer.limit();
			byte[] payload = wPtrToBuffer.array();
			bos.write(limit);
			bos.write(payload);
			bos.flush();
			bos.close();
//			bufferPool.returnBuffer(wPtrToBuffer);
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
