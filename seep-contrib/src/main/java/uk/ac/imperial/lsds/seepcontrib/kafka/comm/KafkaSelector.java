package uk.ac.imperial.lsds.seepcontrib.kafka.comm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.DataStoreSelector;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.errors.NotImplementedException;

public class KafkaSelector implements DataStoreSelector {

	final private static Logger LOG = LoggerFactory.getLogger(KafkaSelector.class);
	
	private final ConsumerConnector consumer;
	private Reader[] readers;
	
	private int numReaderWorkers;
	private Thread[] readerWorkers;
	
	private Map<Integer, IBuffer> dataAdapters;
	
	public KafkaSelector(String baseTopic, String zookeeperServer, String groupId, Map<Integer, IBuffer> dataAdapters) {
		Properties consumerProps = new Properties();
		consumerProps.put("zookeeper.connect", zookeeperServer);
		consumerProps.put("group.id", groupId);
		consumerProps.put("client.id", groupId);
				
		this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
		
		this.dataAdapters = dataAdapters;
		numReaderWorkers = dataAdapters.size();
		readers = new Reader[numReaderWorkers];
		readerWorkers = new Thread[numReaderWorkers];
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
     	for (Entry<Integer, IBuffer> entry : dataAdapters.entrySet()) {
     		// Use one consumer thread per stream
     		topicCountMap.put(baseTopic + String.valueOf(entry.getKey()), new Integer(1));
     	}
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        
        // Create pool of reader thread to serve each topic
        int threadNumber = 0;
        for (Map.Entry<Integer, IBuffer> entry : dataAdapters.entrySet()) {
        	String topic = baseTopic + String.valueOf(entry.getKey());
        	List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        	readers[threadNumber] = new Reader(streams.get(0), entry.getValue());
    		Thread reader = new Thread(readers[threadNumber]);
    		reader.setName("Kafka-Reader-"+threadNumber);
    		readerWorkers[threadNumber] = reader;
    		threadNumber = threadNumber + 1;
        }
	}
	
	@Override
	public DataStoreType type() {
		return DataStoreType.KAFKA;
	}
	
	@Override
	public boolean startSelector() {
		// Start readers
		for(Thread r : readerWorkers){
			LOG.info("Starting reader: {}", r.getName());
			r.start();
		}
		return true;
	}
	
	@Override
	public boolean stopSelector(){
		// TODO: do this
		throw new NotImplementedException("stopKafkaSelector not implemented!!");
	}
	
	@Override
	public boolean initSelector() {
		
		return true;
	}

	class Reader implements Runnable {
		
		private final KafkaStream<byte[], byte[]> stream;
		private final IBuffer ib;
		
		public Reader(KafkaStream<byte[], byte[]> stream, IBuffer ib) {
			this.stream = stream;
			this.ib = ib;
		}
		
		@Override
		public void run() {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while (it.hasNext()) {
	        	byte[] data = it.next().message();

	        	ib.pushData(data);
	        }
		}
		
	}
}
