package uk.ac.imperial.lsds.seepcontrib.kafka.comm;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
import uk.ac.imperial.lsds.seepcontrib.kafka.KafkaSystemProducer;

public class KafkaOutputAdapter implements OutputAdapter {

	final private DataStoreType TYPE = DataStoreType.KAFKA;
	final private KafkaSystemProducer producer;
	
	private int streamId;

	public KafkaOutputAdapter(String kafkaServer, String producerId, String baseTopic, int streamId) {
		this.streamId = streamId;
		producer = new KafkaSystemProducer(kafkaServer, producerId);
		producer.register(baseTopic + String.valueOf(streamId) );
	}
	
	@Override
	public void send(byte[] o) {
		producer.send("key", o);
	}

	@Override
	public void sendAll(byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendKey(byte[] o, int key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendKey(byte[] o, String key) {
		// TODO Auto-generated method stub
	}

	@Override
	public void sendToStreamId(int streamId, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendToAllInStreamId(int streamId, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, int key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, String key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void send_index(int index, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void send_opid(int opId, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public Map<Integer, OutputBuffer> getOutputBuffers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setEventAPI(EventAPI eAPI) {
		// TODO Auto-generated method stub
	}

	@Override
	public DataStoreType getDataOriginType() {
		return TYPE;
	}

}
