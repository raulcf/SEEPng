package uk.ac.imperial.lsds.seepcontrib.kafka.comm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaSelector {

	final private static Logger LOG = LoggerFactory.getLogger(KafkaSelector.class);
	
	private final ConsumerConnector consumer;
	private final String consumerTopic;
	private Reader[] readers;
	private Writer writer;
	
	private int numReaderWorkers;
	private Thread[] readerWorkers;
	private Thread writerWorker;
	
	private Map<Integer, InputAdapter> dataAdapters;
	
	public KafkaSelector(int numReaderWorkers, String consumerTopic, String kafkaServer, String zookeeperServer) {
		Properties consumerProps = new Properties();
		consumerProps.put("zookeeper.connect", zookeeperServer);
		consumerProps.put("group.id", 0);
		
		this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
		this.consumerTopic = consumerTopic;
		
		this.numReaderWorkers = numReaderWorkers;
		readers = new Reader[numReaderWorkers];
		readerWorkers = new Thread[numReaderWorkers];
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
     	topicCountMap.put(consumerTopic, new Integer(numReaderWorkers));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(consumerTopic);
 
        // Create pool of reader thread
        int threadNumber = 0;
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            readers[threadNumber] = new Reader(stream);
            Thread reader = new Thread(readers[threadNumber]);
            reader.setName("Kafka-Reader-"+threadNumber);
        	readerWorkers[threadNumber] = reader;
        }
		
		writer = new Writer();
		writerWorker = new Thread();
	}
	
	public void startKafkaSelector() {
		// Start readers
		for(Thread r : readerWorkers){
			LOG.info("Starting reader: {}", r.getName());
			r.start();
		}
		// TODO Start writer
	}

	public void configureAccept(Map<Integer, InputAdapter> dataAdapters){
		this.dataAdapters = dataAdapters;
	}
	
	class Reader implements Runnable {
		
		private final KafkaStream<byte[], byte[]> stream;
		private boolean working;
		
		public Reader(KafkaStream<byte[], byte[]> stream) {
			this.stream = stream;
		}
		
		public void stop() {
			working = false;
		}
		
		@Override
		public void run() {
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
	        while (it.hasNext()) {
	        	byte[] data = it.next().message();

	        	// TODO something with data
	        	System.out.println("[" + Thread.currentThread().getName() + "] got message: " + data);
	        	
	        	// TODO check mapping between dataAdapters and what?
	        	InputAdapter ia = dataAdapters.get(stream.clientId());
	        	ia.pushData(data);
	        }
		}
		
	}
	
	class Writer implements Runnable {

		@Override
		public void run() {
			// TODO Auto-generated method stub
			
		}
		
	}
}
