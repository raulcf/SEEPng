package uk.ac.imperial.lsds.seepcontrib.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSystemProducer {
	private Producer producer;
	private String topic;
	
	public KafkaSystemProducer(String kafkaSever, String producerId) {
		Properties props = new Properties();
		 
		props.put("bootstrap.servers", kafkaSever);
		props.put("client.id", producerId);
		producer = new KafkaProducer(props);
	}
	
	public void stop() {
		if (producer != null)
			producer.close();
	}
	
	public void register(String topic) {
		this.topic = topic;
	}
	
	public void flush(String source) {
		// TODO
	}
	
	public void send(String key, byte[] message) {
		ProducerRecord record = new ProducerRecord(topic, 0, key.getBytes(), message);
		producer.send(record);
	}
}
