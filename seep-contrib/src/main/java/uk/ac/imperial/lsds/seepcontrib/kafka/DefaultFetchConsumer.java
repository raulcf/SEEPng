package uk.ac.imperial.lsds.seepcontrib.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class DefaultFetchConsumer implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private final KafkaSystemConsumer baseConsumer;
    
    public  DefaultFetchConsumer(KafkaSystemConsumer consumer, KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        
        baseConsumer = consumer;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
        	byte[] data = it.next().message();
        	System.out.println("got message: " + data);
        	baseConsumer.receiveData(data); 
        }
    }
}