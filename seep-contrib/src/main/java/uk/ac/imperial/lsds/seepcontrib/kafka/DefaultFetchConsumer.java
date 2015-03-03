package uk.ac.imperial.lsds.seepcontrib.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class DefaultFetchConsumer implements Runnable {
    private KafkaStream stream;
    private int threadNumber;
    private final KafkaSystemConsumer baseConsumer;
    
    public  DefaultFetchConsumer(KafkaSystemConsumer consumer, KafkaStream stream, int threadNumber) {
        this.threadNumber = threadNumber;
        this.stream = stream;
        this.baseConsumer = consumer;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
        	byte[] data = it.next().message();
        	baseConsumer.receiveData(data); 
        }
    }
}
