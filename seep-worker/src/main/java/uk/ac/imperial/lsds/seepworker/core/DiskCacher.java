package uk.ac.imperial.lsds.seepworker.core;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepworker.WorkerConfig;

/***
 * Class to move Datasets between disk and memory, as requested (by the DataReferenceManager).
 * @author iv
 *
 */
public class DiskCacher {
	
	final private Logger LOG = LoggerFactory.getLogger(DiskCacher.class.getName());
	
	private Map<Integer, String> filenames;
	private static DiskCacher instance;
	
	private WorkerConfig wc;
	
	private DiskCacher(WorkerConfig wc) {
		filenames = new HashMap<Integer, String>();
		this.wc = wc;
	}

	public static DiskCacher makeDiskCacher(WorkerConfig wc) {
		if(instance == null) {
			instance = new DiskCacher(wc);
		}
		return instance;
	}
	
	public String createDatasetOnDisk(int datasetId) {
		String cacheFileName = "";
		
		//Changing to abs might lead to a conflict (HIGHLY unlikely, needs a DataSet with the opposite 
		//ID cached at exactly the same time), but files that start with - are annoying in console
		//debugging.
		cacheFileName = Math.abs(datasetId) + "_" + System.currentTimeMillis() + ".cached";
		filenames.put(datasetId, cacheFileName);
		
		return cacheFileName;
	}
	
	private String getCacheFileName(int id) {
		String cacheFileName = "";
		if(filenames == null) {
			System.out.println("filenames null");
		}
		if (filenames.containsKey(id)) {
			//Already on disk. We could claim victory and return, but this will allow us to cache any
			//items stuck in memory (see comment below).
			cacheFileName = filenames.get(id);
		} 
		else {
			//Changing to abs might lead to a conflict (HIGHLY unlikely, needs a DataSet with the opposite 
			//ID cached at exactly the same time), but files that start with - are annoying in console
			//debugging.
			cacheFileName = Math.abs(id) + "_" + System.currentTimeMillis() + ".cached";
			filenames.put(id, cacheFileName);
		}
		return cacheFileName;
	}
	
	public int cacheToDisk(Dataset data) throws FileNotFoundException, IOException {
		String cacheFileName = getCacheFileName(data.id());
		
		// Prepare channel
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(cacheFileName), wc.getInt(WorkerConfig.BUFFERPOOL_MIN_BUFFER_SIZE));
		
		// Basically get buffers from Dataset and write them in chunks, and ordered to disk
		Iterator<ByteBuffer> buffers = data.prepareForTransferToDisk();
		
		while(buffers.hasNext()) {
			ByteBuffer bb = buffers.next();
			byte[] payload = bb.array();
			int limit = bb.limit();
			bos.write(limit);
			bos.write(payload, 0, payload.length);
		}
		
		// close
		int freedMemory = data.completeTransferToDisk();
		
		bos.flush();
		bos.close();
		
		data.setCachedLocation(cacheFileName);
		LOG.debug("Content is spilled to: {}", cacheFileName);
		
		return freedMemory;
	}
	
	/***
	 * Moves a Dataset to disk. If the Dataset is already on disk we try to move everything to
	 * disk anyway. This is because there can be some rare instances with multithreading when
	 * a Dataset can retain something in memory after being sent to disk. Two calls to this
	 * function solve that if the user wants to be sure (subsequent calls do nothing), although
	 * there is no guarantee on the order of records.
	 * @param data
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @return The number of records sent to disk.
	 */
	public int _cacheToDisk(Dataset data) throws FileNotFoundException, IOException {
		String cacheFileName = "";
		int cachedRecords = 0;
		//if(data.id() == null) {System.out.println("ID");}
		if(filenames == null) {
			System.out.println("filenames null");
		}
		if(data == null) {
			System.out.println("data null");
		}
		if (filenames.containsKey(data.id())) {
			//Already on disk. We could claim victory and return, but this will allow us to cache any
			//items stuck in memory (see comment below).
			cacheFileName = filenames.get(data.id());
		} else {
			//Changing to abs might lead to a conflict (HIGHLY unlikely, needs a DataSet with the opposite 
			//ID cached at exactly the same time), but files that start with - are annoying in console
			//debugging.
			cacheFileName = Math.abs(data.id()) + "_" + System.currentTimeMillis() + ".cached";
			filenames.put(data.id(), cacheFileName);
		}
		try {
			DataOutputStream cacheStream = new DataOutputStream(new FileOutputStream(cacheFileName));
			byte[] record;
			while((record = data.consumeDataFromMemoryForCopy()) != null) {
				cacheStream.writeInt(record.length);
				cacheStream.write(record);
				cachedRecords++;
			}
			LOG.debug("Cached records: {}", cachedRecords);
			cacheStream.flush();
			cacheStream.close();
			//Setting this late makes the implementation thread safe-ish. Any writes to the Dataset will
			//start going to the file NOW, so it won't try to write to disk at the same time as this function.
			//HOWEVER, any writes that happened between the last write and now will be stuck in memory.
			//A second call to cacheToDisk can solve this, although data might be reordered if Dataset.write
			//is called in the meantime.
			data.setCachedLocation(cacheFileName);
			LOG.debug("Content is spilled to: {}", cacheFileName);
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
		return cachedRecords;
	}
	
	public void retrieveFromDisk(Dataset data) throws FileNotFoundException {
		
		// Get cache file
		String cacheFileName = filenames.get(data.id());
		// Prepare dataset for trasnfer to memory
		ByteBuffer currentPointer = data.prepareForTransferToMemory();
		
		int bbSize = wc.getInt(WorkerConfig.BUFFERPOOL_MIN_BUFFER_SIZE);
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(cacheFileName), bbSize);

		data.transferToMemory(bis, bbSize);
		
		data.completeTransferToMemory(currentPointer);
		
		data.unsetCachedLocation();
		filenames.remove(data.id());
		if (filenames.containsKey(data.id())) {
			data.setCachedLocation(filenames.get(data.id()));
		}
	}
	
	/**
	 * Returns a Dataset from disk to memory.
	 * @param data
	 * @return The number of records returned to memory.
	 */
	public int _retrieveFromDisk(Dataset data) {
		if (!filenames.containsKey(data.id())) {
			//No file on disk, so exactly zero ITuples can be returned to memory.
			return 0;
		}
		FileInputStream inputStream  = null;
		int returnedTuples = 0;
		String cacheFileName = filenames.get(data.id());
		try {
			// We reset the structures that were used by this dataset while still living in memory
//			data.resetDataset();
			//Unsetting this first is necessary - otherwise the write we use to place the record
			//back in memory will just be appended back to the file.
			long filePosition = data.cacheFileLocation();
			data.unsetCachedLocation();
			//It is used, in the while condition just below, but Eclipse's analyzer isn't 
			//smart enough to figure that out. This just saves us a warning.
			@SuppressWarnings("unused")
			int readSuccess;
			inputStream = new FileInputStream(cacheFileName);
			inputStream.getChannel().position(filePosition);
			byte[] recordSizeBytes = new byte[Integer.SIZE / Byte.SIZE];
			//If there is another record the next few bytes will be an int containing the size of said record.
			while ((readSuccess = inputStream.read(recordSizeBytes)) != -1) {
				//Convert the bytes giving us the size to an int and read exactly the next record
				int recordSize = ByteBuffer.wrap(recordSizeBytes).getInt();
				byte[] record = new byte[recordSize + Integer.SIZE / Byte.SIZE];
				System.arraycopy(recordSizeBytes, 0, record,0, Integer.SIZE / Byte.SIZE);
				inputStream.read(record, Integer.SIZE / Byte.SIZE, recordSize);
				
				// TODO: No RuntimeEventRegister for now. Does it make sense? Raul: yes, it makes sense
				data.write(record, null);
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
			filenames.remove(data.id());
			return returnedTuples;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException ioe) {
			// TODO Auto-generated catch block
			ioe.printStackTrace();
		} 
		if (filenames.containsKey(data.id())) {
			data.setCachedLocation(filenames.get(data.id()));
		}
		try {
			if (inputStream != null) {
				inputStream.close();
			}
		} catch (IOException ex) {
			
		}
		return returnedTuples;
	}
	
	/***
	 * Check if a Dataset is currently entirely in memory (or conversely, 
	 * (perhaps partially in the case of multithreading) on disk).
	 * @param data
	 * @return true if data is in memory.
	 */
	public boolean inMem(Dataset data) {
		return (!(filenames.containsKey(data.id())));
	}

}
