package uk.ac.imperial.lsds.seepworker.core;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;

// No thread safe. In particular no simultaneous write and read is allowed right now
public class Dataset implements IBuffer, OBuffer {

	private int id;
	private DataReference dataReference;
	private BufferPool bufferPool;
	
	private Queue<ByteBuffer> buffers;
	private Iterator<ByteBuffer> readerIterator;
	private ByteBuffer wPtrToBuffer;
	private ByteBuffer rPtrToBuffer;
	private String cacheFileName = "";

	public Dataset(DataReference dataReference, BufferPool bufferPool) {
		this.dataReference = dataReference;
		this.id = dataReference.getId();
		this.bufferPool = bufferPool;
		this.wPtrToBuffer = bufferPool.borrowBuffer();
		this.buffers = new LinkedList<>();
		this.buffers.add(wPtrToBuffer);
	}
	
	public Dataset(int id, byte[] syntheticData, DataReference dr, BufferPool bufferPool) {
		this.dataReference = dr;
		this.id = id;
		this.bufferPool = bufferPool;
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
					//Yes, the following looks a bit silly, but it is necessary to allow
					//readerIterator.remove to work without the iterator complaining
					//about concurrent modification
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
	
	public void cacheToDisk() throws FileNotFoundException, IOException {
		if (!cacheFileName.equals("")) {
			//Already on disk. Claim victory and move on.
			return;
		}
		//Changing to abs might lead to a conflict (HIGHLY unlikely, needs a DataSet with the opposite 
		//ID cached at exactly the same time), but files that start with - are annoying in console
		//debugging.
		cacheFileName = Math.abs(id) + "_" + System.currentTimeMillis() + ".cached";
		try {
			DataOutputStream cacheStream = new DataOutputStream(new FileOutputStream(cacheFileName));
			byte[] record;
			while((record = consumeData()) != null) {
				cacheStream.writeInt(record.length);
				cacheStream.write(record);
			}
			cacheStream.flush();
			cacheStream.close();
		} catch (FileNotFoundException fnfe) {
			cacheFileName = "";
			fnfe.printStackTrace();
			throw fnfe;
		} catch (IOException ioe) {
			//Assume nothing got cached. This isn't a particularly safe assumption, but it's probably
			//more likely than something was cached and we have a split dataset.
			cacheFileName = "";
			ioe.printStackTrace();
			throw ioe;
		} catch (SecurityException e) {
			cacheFileName = "";
			//Yes, technically throwing the SecurityException would be more descriptive, but the end result is the same,
			//and handling only one type of exception is easier at the higher levels.
			FileNotFoundException toThrow = new FileNotFoundException("Security settings prevent the file from being created");
			toThrow.fillInStackTrace();
			toThrow.printStackTrace();
			throw toThrow;
		}
	}
	
	public int retrieveFromDisk() {
		if (cacheFileName.equals("")) {
			//No file on disk, so exactly zero ITuples can be returned to memory.
			return 0;
		}
		FileInputStream inputStream  = null;
		int returnedTuples = 0;
		try {
			//It is used, in the while condition just below, but Eclipse's analyzer isn't 
			//smart enough to figure that out. This just saves us a warning.
			@SuppressWarnings("unused")
			int readSuccess;
			inputStream = new FileInputStream(cacheFileName);
			byte[] recordSizeBytes = new byte[Integer.SIZE / Byte.SIZE];
			//If there is another record the next few bytes will be an int containing the size of said record.
			while ((readSuccess = inputStream.read(recordSizeBytes)) != -1) {
				//Convert the bytes giving us the size to an int and read exactly the next record
				int recordSize = ByteBuffer.wrap(recordSizeBytes).getInt();
				byte[] record = new byte[recordSize];
				inputStream.read(record);
				
				if(wPtrToBuffer.remaining() < recordSize + TupleInfo.TUPLE_SIZE_OVERHEAD) {
					// Borrow a new buffer and add to the collection
					this.wPtrToBuffer = bufferPool.borrowBuffer();
					this.buffers.add(wPtrToBuffer);
				}
				
				wPtrToBuffer.putInt(recordSize);
				wPtrToBuffer.put(record);
				returnedTuples++;
			}
			inputStream.close();
			try {
				Files.delete((new File(cacheFileName)).toPath());
			} catch (IOException ioex) {
				//This isn't good, but failing to clean up is different from not being able
				//to read everything, so we still consider this a success.
				//ioex.printStackTrace();
			}
			cacheFileName = "";
			return returnedTuples;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException ioe) {
			// TODO Auto-generated catch block
			ioe.printStackTrace();
		} 
		try {
			if (inputStream != null) {
				inputStream.close();
			}
		} catch (IOException ex) {
			
		}
		return returnedTuples;
	}
	
	public boolean inMem() {
		return cacheFileName.equals("");
	}
}
