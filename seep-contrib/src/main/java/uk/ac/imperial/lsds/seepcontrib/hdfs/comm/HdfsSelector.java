package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.FileConfig;
import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepcontrib.hdfs.config.HdfsConfig;
import uk.ac.imperial.lsds.seepcontrib.kafka.comm.KafkaSelector;

public class HdfsSelector {
	final private static Logger LOG = LoggerFactory.getLogger(HdfsSelector.class);
	
	private int numUpstreamResources;
	
	private Reader reader;
	private Thread readerWorker;
	private Writer writer;
	private Thread writerWorker;
	private int headroom;
	private boolean source;
	
	private Map<Integer, InputAdapter> dataAdapters;
	
	public HdfsSelector(int headroom) {
		source = false;
		this.headroom = headroom;
	}
	
	public void startHdfsSelector() {
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
	public void configureAccept(Map<Integer, DataStore> fileOrigins, Map<Integer, InputAdapter> dataAdapters){
		this.dataAdapters = dataAdapters;
		this.numUpstreamResources = fileOrigins.size();
		this.reader = new Reader();
		this.readerWorker = new Thread(this.reader);
		this.readerWorker.setName("HdfsFile-Reader");
		
		Map<ReadableByteChannel, Integer> streams = new HashMap<>();
		Map<FSDataInputStream, Integer> streamin = new HashMap<>();
		int size = fileOrigins.entrySet().size();

		for(Entry<Integer, DataStore> e : fileOrigins.entrySet()){
			try {
				HdfsConfig config = new HdfsConfig(e.getValue().getConfig());
				String absPath = Utils.absolutePath(config.getString(HdfsConfig.HDFS_PATH));
				absPath = config.getString(HdfsConfig.HDFS_SERVER)+absPath;
				URI uri = new URI("hdfs://" + absPath);
				LOG.info("Created URI to local resource: {}", uri.toString());
				Path resource = new Path(uri);
				Configuration conf = new Configuration();  
				FileSystem fs = FileSystem.get(uri, conf);
				FSDataInputStream hdfsInStream = fs.open(resource);
				InputStream in = hdfsInStream;
				if(config.getString(HdfsConfig.HDFS_TEXT).compareTo("TEXT")==0){
					LOG.info("*****************TEXT FORM*****************");
					this.reader.text();
				}
				ReadableByteChannel rbc = Channels.newChannel(in);
				streams.put(rbc, e.getKey());
				streamin.put(hdfsInStream, e.getKey());
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
		this.reader.availableChannels(streams,streamin);
	}
	public void stopHdfsSelector(){
		// TODO: do this
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

	class Reader implements Runnable {
		
		private boolean working = true;
		private Selector readSelector;
		private Map<ReadableByteChannel, Integer> channels;
		private Map<FSDataInputStream,Integer> streams;
		private boolean isText = false;
		
		public void text(){
			isText = true;
		}
		public Reader() {
			try {
				this.readSelector = Selector.open();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void availableChannels(Map<ReadableByteChannel, Integer> channels,Map<FSDataInputStream,Integer> streamin){
			this.channels = channels;
			this.streams = streamin;
		}
		
		public void stop(){
			this.working = false;
		}
		public void run(){
			LOG.info("Started HdfsFile Reader worker: {}", Thread.currentThread().getName());
			while(working){
				if(isText){
					for(Entry<FSDataInputStream, Integer> set: streams.entrySet()){
						int id = set.getValue();
						FSDataInputStream fsin = set.getKey();
						InputAdapter ia = dataAdapters.get(id);
						char a = '0';
						try {
						if(fsin.available()>0){
							a = (char) fsin.readByte();
							}
						else{
							working = false;
						}
			
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						byte[] onedata = {(byte)a,0,0,0,0,0,0,0};
						ia.pushData(onedata);
					}
				}
				else
				{
				for(Entry<ReadableByteChannel, Integer> set: channels.entrySet()){
					int id = set.getValue();
					ReadableByteChannel rbc = set.getKey();
					InputAdapter ia = dataAdapters.get(id);
					if(rbc.isOpen()){
						ia.readFrom(rbc, id);
					}
					else{
						working = false;
					}
				}
			}
			}
			LOG.info("Finished HdfsFile Reader worker: {}", Thread.currentThread().getName());
			this.closeReader();
			
		}
		private void closeReader(){
			for(ReadableByteChannel rbc : channels.keySet()){
				try {
					rbc.close();
				
				}		catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				}
			
			for(FSDataInputStream in : streams.keySet()){
				
						try {
							in.close();
						} catch (IOException e) {
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
	public void source() {
		source = true;// TODO Auto-generated method stub
		
	}
}