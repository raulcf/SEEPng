package uk.ac.imperial.lsds.seepcontrib.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaSystemConsumer {
	//private KafkaConsumer consumer;
	private final ConsumerConnector consumer;
	private final int numThreads = 1;
	private final SeepTask seepTask;
	private final Schema schema;
	private ExecutorService executor;
	private String topic;
	
	public KafkaSystemConsumer(String zookeeperServer, String consumerId, SeepTask seepTask, Schema schema) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeperServer);
		props.put("group.id", consumerId);
		
		this.seepTask = seepTask;
		this.schema = schema;
		this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}
	
	public void stop() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}
	
	public void subscribe(String topic) {
		this.topic = topic;
	}
	
	public void start() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // launch sink threads
        executor = Executors.newFixedThreadPool(numThreads);
 
        // now create an object to consume the messages
        int threadNumber = 0;
        for (final KafkaStream<?, ?> stream : streams) {
            executor.submit(new DefaultFetchConsumer(this, stream, threadNumber));
            threadNumber++;
        }
    }
	
	protected void receiveData(byte[] data) {
		ITuple iTuple = new ITuple(schema);
		iTuple.setData(data);
		// TO DO: use the api to pass data
		seepTask.processData(iTuple, null);
	}
}
