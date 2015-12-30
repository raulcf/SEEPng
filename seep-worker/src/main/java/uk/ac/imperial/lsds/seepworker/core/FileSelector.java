package uk.ac.imperial.lsds.seepworker.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileConfig;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class FileSelector implements DataStoreSelector, EventAPI {

	final private static Logger LOG = LoggerFactory.getLogger(FileSelector.class);
	
	private int numUpstreamResources;
	
	private Reader reader;
	private Thread readerWorker;
	private Writer writer;
	private Thread writerWorker;
	
	private Map<Integer, IBuffer> dataAdapters;
	
	private Map<Integer, SelectionKey> writerKeys;
	
	public FileSelector(WorkerConfig wc) {
		this.writerKeys = new HashMap<>();
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
		
		// Stop writers
		if(writerWorker != null){
			LOG.info("Stopping writer: {}", writerWorker.getName());
			writer.stop();
		}
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
		this.numUpstreamResources = fileOrigins.size();
		this.reader = new Reader();
		this.readerWorker = new Thread(this.reader);
		this.readerWorker.setName("File-Reader");
		
		Map<SeekableByteChannel, Integer> channels = new HashMap<>();
		for(Entry<Integer, DataStore> e : fileOrigins.entrySet()){
			try {
				FileConfig config = new FileConfig(e.getValue().getConfig());
				String absPath = Utils.absolutePath(config.getString(FileConfig.FILE_PATH));
				URI uri = new URI(Utils.FILE_URI_SCHEME + absPath);
				LOG.info("Created URI to local resource: {}", uri.toString());
				Path resource = Paths.get(uri);
				LOG.info("Configuring file channel: {}", resource.toString());
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
		}
		this.reader.availableChannels(channels);
	}
	
	public void addNewAccept(Path resource, int id, Map<Integer, IBuffer> dataAdapters) {
		this.dataAdapters = dataAdapters;
		Map<SeekableByteChannel, Integer> channels = new HashMap<>();
		SeekableByteChannel sbc = null;
		try {
			sbc = Files.newByteChannel(resource, StandardOpenOption.READ);
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		channels.put(sbc, id);
		this.reader = new Reader();
		this.readerWorker = new Thread(this.reader);
		this.readerWorker.setName("File-Reader");
		this.reader.availableChannels(channels);
	}
	
	public void configureDownstreamFiles(Map<Integer, DataStore> fileDest, Set<OBuffer> obufsToStream) {
		// Lazily configure the writer
		this.writer = new Writer();
		this.writerWorker = new Thread(this.writer);
		this.writerWorker.setName("File-Writer");
		// Notify the writer of a new set of downstream files
		this.writer.newDownstreamFile(fileDest, obufsToStream);
	}
	
	@Override
	public void readyForWrite(int id) {
		writerKeys.get(id).selector().wakeup();
	}

	@Override
	public void readyForWrite(List<Integer> ids) {
		for(Integer id : ids){
			readyForWrite(id);
		}
	}
	
	class Reader implements Runnable {

		private boolean working = true;
		private Selector readSelector;
		private Map<SeekableByteChannel, Integer> channels;
		
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
		
		public void stop() {
			this.working = false;
		}
		
		@Override
		public void run() {
			LOG.info("Started File Reader worker: {}", Thread.currentThread().getName());
			while(working){
				for(Entry<SeekableByteChannel, Integer> e: channels.entrySet()) {
					int id = e.getValue();
					ReadableByteChannel rbc = e.getKey();
					IBuffer ib = dataAdapters.get(id);
					if(rbc.isOpen()){
						ib.readFrom(rbc);
					}
					else{
						working = false;
					}
				}
			}
			LOG.info("Finished File Reader worker: {}", Thread.currentThread().getName());
			this.closeReader();
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
	
	class Writer implements Runnable {

		private boolean working = true;
		private Selector writeSelector;
		private Map<SeekableByteChannel, Integer> channels;
		
		public void stop(){
			// TODO: implement
		}
		
		public void newDownstreamFile(Map<Integer, DataStore> fileDest, Set<OBuffer> obufsToStream) {
			// Create fileChannels for each of these files
			for(Entry<Integer, DataStore> entry : fileDest.entrySet()) {
				int id = entry.getKey(); // streamId is the key here
				OBuffer oBuffer = getOBufferWithId(obufsToStream, id);
				
				DataStore ds = entry.getValue();
				String path = ds.getConfig().getProperty(FileConfig.FILE_PATH);
				String pathAndFilename = path + id;
				Path p = FileSystems.getDefault().getPath(pathAndFilename);
				WritableByteChannel channel = null;
				try {
					channel = FileChannel.open(p);
					
				} 
				catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// Register channel with the selector
				SelectionKey key = null;
				try {
					key = ((SelectableChannel) channel).register(
							writeSelector, 
							SelectionKey.OP_WRITE);
				} 
				catch (ClosedChannelException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// Attach the OBuffer with the key, so that writer knows from where to read
				key.attach(oBuffer);
				LOG.info("Configured new output file OP: {} to {}", oBuffer.id(), p.toString());
			}			
		}
		
		private OBuffer getOBufferWithId(Set<OBuffer> bufs, int id) {
			for(OBuffer ob : bufs) {
				if(ob.id() == id) {
					return ob;
				}
			}
			return null;
		}

		@Override
		public void run() {
			LOG.info("Started File Writer worker: {}", Thread.currentThread().getName());
			while(working) {
				
				Set<SelectionKey> selectedKeys = writeSelector.selectedKeys();
				Iterator<SelectionKey> keyIt = selectedKeys.iterator();
				while(keyIt.hasNext()) {
					SelectionKey key = keyIt.next();
					keyIt.remove();
					// writable
					if(key.isWritable()) {
						OBuffer ob = (OBuffer)key.attachment();
						WritableByteChannel channel = (WritableByteChannel)key.channel();
						if(channel.isOpen()) {
							boolean fullyWritten = ob.drainTo(channel);
							if(fullyWritten) unsetWritable(key);
						}
						else {
							LOG.error("Closed destiny file");
						}
					}
					if(! key.isValid()){
						String conn = ((WritableByteChannel)key.channel()).toString();
						LOG.warn("Invalid outgoing data connection to: {}", conn);
					}
				}
			}
			LOG.info("Finished File Reader worker: {}", Thread.currentThread().getName());
			this.closeWriter();
		}
		
		private void unsetWritable(SelectionKey key){
			final int newOps = key.interestOps() & ~SelectionKey.OP_WRITE;
			key.interestOps(newOps);
		}
		
		private void closeWriter(){
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
}
