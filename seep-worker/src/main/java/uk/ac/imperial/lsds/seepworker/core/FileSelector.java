package uk.ac.imperial.lsds.seepworker.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.Selector;
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
import uk.ac.imperial.lsds.seep.api.FileConfig;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class FileSelector {

	final private static Logger LOG = LoggerFactory.getLogger(FileSelector.class);
	
	private int numUpstreamResources;
	
	private Reader reader;
	private Thread readerWorker;
	private Writer writer;
	private Thread writerWorker;
	
	private Map<Integer, InputAdapter> dataAdapters;
	
	public FileSelector(WorkerConfig wc) {
		
	}
	
	public void startFileSelector(){
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
	}
	
	public void stopFileSelector(){
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
	}
	
	public void configureAccept(Map<Integer, DataStore> fileOrigins, Map<Integer, InputAdapter> dataAdapters){
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
	
	public void addNewAccept(Path resource, int id, Map<Integer, InputAdapter> dataAdapters){
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
	
	public void configureDownstreamFiles(Map<Integer, DataStore> fileDest){
		// TODO: implement this, configure writer, etc...
		throw new NotImplementedException("TODO: ");
	}
	
	class Reader implements Runnable {

		private boolean working = true;
		private Selector readSelector;
		private Map<SeekableByteChannel, Integer> channels;
		
		public Reader(){
			try {
				this.readSelector = Selector.open();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public void availableChannels(Map<SeekableByteChannel, Integer> channels){
			this.channels = channels;
		}
		
		public void stop(){
			this.working = false;
		}
		
		@Override
		public void run() {
			LOG.info("Started File Reader worker: {}", Thread.currentThread().getName());
			while(working){
				for(Entry<SeekableByteChannel, Integer> e: channels.entrySet()){
					int id = e.getValue();
					ReadableByteChannel rbc = e.getKey();
					InputAdapter ia = dataAdapters.get(id);
					if(rbc.isOpen()){
						ia.readFrom(rbc, id);
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

		public void stop(){
			// TODO: implement
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			
		}
	}
}
