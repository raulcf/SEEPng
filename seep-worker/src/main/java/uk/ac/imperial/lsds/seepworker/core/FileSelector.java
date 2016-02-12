package uk.ac.imperial.lsds.seepworker.core;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.Selector;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileConfig;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

// TODO: can we create an inputBuffer that knows how to read directly from file?
public class FileSelector implements DataStoreSelector {

	final private static Logger LOG = LoggerFactory.getLogger(FileSelector.class);
	
//	private int numUpstreamResources;
	
	private Reader reader;
	private Thread readerWorker;
//	private Writer writer;
	private Thread writerWorker;
	
	private Map<Integer, IBuffer> dataAdapters;
	
	private String defaultCharacterSet = Charset.defaultCharset().name();
	
//	private Map<Integer, SelectionKey> writerKeys;
	
	public FileSelector(WorkerConfig wc) {
//		this.writerKeys = new HashMap<>();
	}
	
	@Override
	public boolean startSelector() {
		// Start readers
		if(readerWorker != null){
			LOG.info("Starting reader: {}", readerWorker.getName());
			readerWorker.start();
		}
	
		// Start writers
		if(writerWorker != null){
			LOG.info("Starting writer: {}", writerWorker.getName());
			writerWorker.start();
		}
		return true;
	}
	
	@Override
	public boolean stopSelector() {
		// Stop readers
		if(readerWorker != null){
			LOG.info("Stopping reader: {}", readerWorker.getName());
			reader.stop();
		}
		
//		// Stop writers
//		if(writerWorker != null){
//			LOG.info("Stopping writer: {}", writerWorker.getName());
//			writer.stop();
//		}
		return true;
	}
	
	@Override
	public DataStoreType type() {
		return DataStoreType.FILE;
	}
	
	@Override
	public boolean initSelector() {
		
		return true;
	}
	
	public void configureAccept(Map<Integer, DataStore> fileOrigins, Map<Integer, IBuffer> dataAdapters){
		this.dataAdapters = dataAdapters;
//		this.numUpstreamResources = fileOrigins.size();
		this.reader = new Reader();
		this.readerWorker = new Thread(this.reader);
		this.readerWorker.setName("File-Reader");

		Map<SeekableByteChannel, Integer> channels = new HashMap<>();
		for(Entry<Integer, DataStore> e : fileOrigins.entrySet()){
			try {
				FileConfig config = new FileConfig(e.getValue().getConfig());
				//String absPath = Utils.absolutePath(config.getString(FileConfig.FILE_PATH));
				String absPath = config.getString(FileConfig.FILE_PATH);
				URI uri = new URI(Utils.FILE_URI_SCHEME + absPath);
				LOG.info("Created URI to local resource: {}", uri.toString());
				Path resource = Paths.get(uri);
				defaultCharacterSet = config.getString(FileConfig.CHARACTER_SET);
				LOG.info("Configuring file channel: {} as binary input", resource.toString());
				SeekableByteChannel sbc = Files.newByteChannel(resource, StandardOpenOption.READ);
				channels.put(sbc, e.getKey());
			} 
			catch (FileNotFoundException fnfe) {
				fnfe.printStackTrace();
			}
			catch (URISyntaxException use) {
				use.printStackTrace();
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
			catch (IllegalCharsetNameException icne) {
				icne.printStackTrace();
			}
			catch (IllegalArgumentException iae) {
				iae.printStackTrace();
			}
		}
		this.reader.availableChannels(channels);
		this.reader.availableDataStores(fileOrigins);
	}
	
	public void addNewAccept(Path resource, int id, Map<Integer, IBuffer> dataAdapters) {
		//Call the new version of the function for consistency/maintainability. This function exists
		//to default to binary input for reasons of backwards compatibility.
		addNewAccept(resource, id, dataAdapters, false);
	}
	
	public void addNewAccept(Path resource, int id, Map<Integer, IBuffer> dataAdapters, boolean textSource) {
		//Call the more explicit version of the function for consistency/maintainability.
		//Uses the character set specified in configureAccept as the default.
		addNewAccept(resource, id, dataAdapters, false, defaultCharacterSet);
	}
	
	public void addNewAccept(Path resource, int id, Map<Integer, IBuffer> dataAdapters, boolean textSource, String characterSet) {
		this.dataAdapters = dataAdapters;
		Map<SeekableByteChannel, Integer> channels = new HashMap<>();
		//SeekableByteChannel sbc = null;
		try {
			LOG.info("Configuring file channel: {}", resource.toString());
			SeekableByteChannel sbc = Files.newByteChannel(resource, StandardOpenOption.READ);
			channels.put(sbc, id);
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.reader = new Reader();
		this.readerWorker = new Thread(this.reader);
		this.readerWorker.setName("File-Reader");
		this.reader.availableChannels(channels);
		//TODO: set a DataStore so we can grab a Schema
	}
	
//	public void configureDownstreamFiles(Map<Integer, DataStore> fileDest, Set<OBuffer> obufsToStream) {
//		// Lazily configure the writer
//		this.writer = new Writer();
//		this.writerWorker = new Thread(this.writer);
//		this.writerWorker.setName("File-Writer");
//		// Notify the writer of a new set of downstream files
//		this.writer.newDownstreamFile(fileDest, obufsToStream);
//	}
	
//	@Override
//	public void readyForWrite(int id) {
//		writerKeys.get(id).selector().wakeup();
//	}
//
//	@Override
//	public void readyForWrite(List<Integer> ids) {
//		for(Integer id : ids){
//			readyForWrite(id);
//		}
//	}
	
	class Reader implements Runnable {

		private boolean working = true;
		private Selector readSelector;
		private Map<SeekableByteChannel, Integer> channels;
		private Map<Integer, DataStore> channelDataStore;
		
		public Reader() {
			try {
				this.readSelector = Selector.open();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void availableChannels(Map<SeekableByteChannel, Integer> channels) {
			this.channels = channels;
		}
		
		public void availableDataStores(Map<Integer, DataStore> channelDataStore) {
			this.channelDataStore = channelDataStore;
		}
		
		public void stop() {
			this.working = false;
		}
		
		@Override
		public void run() {
			LOG.info("Starting File Reader worker: {}, {} channels",
					 Thread.currentThread().getName(),
					 channels.entrySet().size());
			while(working && channels.entrySet().size() > 0){
				for(Entry<SeekableByteChannel, Integer> e: channels.entrySet()) {
					int id = e.getValue();
					ReadableByteChannel rbc = e.getKey();
					IBuffer ib = dataAdapters.get(id);
					if(rbc.isOpen()) {
						if (isTextSource(e)) {
							readFromText(e, ib, rbc);
							working = false;
						} else {
							int totalTuplesRead = ib.readFrom(rbc);
							if(totalTuplesRead == 0) {
								// Once the file is finished we can stop working here
								working = false;
							}
						}
					} else {
						working = false;
					}
				}
			}
			LOG.info("Finished text File Reader worker: {}", Thread.currentThread().getName());
			this.closeReader();
		}
		
		private boolean isTextSource(Entry<SeekableByteChannel, Integer> e) {
			if (channelDataStore.containsKey(e.getValue())) {
				FileConfig config = new FileConfig(channelDataStore.get(e.getValue()).getConfig());
				defaultCharacterSet = config.getString(FileConfig.CHARACTER_SET);
				return config.getBoolean(FileConfig.TEXT_SOURCE);
			}
			return false;
		}
		
		private void readFromText(Entry<SeekableByteChannel, Integer> e, IBuffer ib, ReadableByteChannel rbc) {
			BufferedReader br = new BufferedReader (Channels.newReader(rbc, channelDataStore.get(e.getValue()).getSchema().getSchemaParser().getCharset().name()));
			String line;
			try {
				while ((line = br.readLine()) != null) {
					ib.pushData(channelDataStore.get(e.getValue()).getSchema().getSchemaParser().bytesFromString(line));
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		}
		
		private void closeReader(){
			for(SeekableByteChannel sbc : channels.keySet()){
				try {
					sbc.close();
				} 
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
//	class Writer implements Runnable {
//
//		private boolean working = true;
//		private Selector writeSelector;
//		private Map<SeekableByteChannel, Integer> channels;
//		
//		/**
//		 * Close all files and then release any remaining resources
//		 * Keep in mind that this does not wait for potential pending data to be written
//		 */
//		public void stop(){
//			this.working = false; // stop worker
//		}
//		
//		public void newDownstreamFile(Map<Integer, DataStore> fileDest, Set<OBuffer> obufsToStream) {
//			// Create fileChannels for each of these files
//			for(Entry<Integer, DataStore> entry : fileDest.entrySet()) {
//				int id = entry.getKey(); // streamId is the key here
//				OBuffer oBuffer = getOBufferWithId(obufsToStream, id);
//				
//				DataStore ds = entry.getValue();
//				String path = ds.getConfig().getProperty(FileConfig.FILE_PATH);
//				String pathAndFilename = path + id;
//				Path p = FileSystems.getDefault().getPath(pathAndFilename);
//				WritableByteChannel channel = null;
//				try {
//					// Create File first
//					boolean created = p.toFile().createNewFile();
//					if(created) {
//						channel = FileChannel.open(p);
//					}
//					else{
//						LOG.error("PANIC: could not create the file");
//						System.exit(-1);
//					}
//				}
//				catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				
//				// Register channel with the selector
//				SelectionKey key = null;
//				try {
//					/**
//					 * this doesnt work, instead create a normal buffered stream
//					 * and hope the os knows how to operate this
//					 */
//					key = ((SelectableChannel) channel).register(
//							writeSelector, 
//							SelectionKey.OP_WRITE);
//				} 
//				catch (ClosedChannelException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				
//				// Attach the OBuffer with the key, so that writer knows from where to read
//				key.attach(oBuffer);
//				LOG.info("Configured new output file OP: {} to {}", oBuffer.id(), p.toString());
//			}			
//		}
//		
//		private OBuffer getOBufferWithId(Set<OBuffer> bufs, int id) {
//			for(OBuffer ob : bufs) {
//				if(ob.id() == id) {
//					return ob;
//				}
//			}
//			return null;
//		}
//
//		@Override
//		public void run() {
//			LOG.info("Started File Writer worker: {}", Thread.currentThread().getName());
//			while(working) {
//				
//				Set<SelectionKey> selectedKeys = writeSelector.selectedKeys();
//				Iterator<SelectionKey> keyIt = selectedKeys.iterator();
//				while(keyIt.hasNext()) {
//					SelectionKey key = keyIt.next();
//					keyIt.remove();
//					// writable
//					if(key.isWritable()) {
//						OBuffer ob = (OBuffer)key.attachment();
//						WritableByteChannel channel = (WritableByteChannel)key.channel();
//						if(channel.isOpen()) {
//							boolean fullyWritten = ob.drainTo(channel);
//							if(fullyWritten) unsetWritable(key);
//						}
//						else {
//							LOG.error("Closed destiny file");
//						}
//					}
//					if(! key.isValid()){
//						String conn = ((WritableByteChannel)key.channel()).toString();
//						LOG.warn("Invalid outgoing data connection to: {}", conn);
//					}
//				}
//			}
//			LOG.info("Finished File Reader worker: {}", Thread.currentThread().getName());
//			this.closeWriter();
//		}
//		
//		private void unsetWritable(SelectionKey key){
//			final int newOps = key.interestOps() & ~SelectionKey.OP_WRITE;
//			key.interestOps(newOps);
//		}
//		
//		private void closeWriter(){
//			try {
//				// Close all files
//				for(SelectionKey key : writeSelector.keys()) {
//					key.channel().close();
//					key.cancel();
//				}
//				writeSelector.close();
//			} 
//			catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}
}
