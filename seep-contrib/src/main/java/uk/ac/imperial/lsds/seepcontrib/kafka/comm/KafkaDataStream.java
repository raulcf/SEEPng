package uk.ac.imperial.lsds.seepcontrib.kafka.comm;

import java.nio.channels.ReadableByteChannel;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataOriginType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.InputAdapter;

public class KafkaDataStream implements InputAdapter {

	public KafkaDataStream(int opId, int streamId, Schema expectedSchema) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Integer> getRepresentedOpId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getStreamId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short returnType() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public DataOriginType getDataOriginType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void readFrom(ReadableByteChannel channel, int id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void pushData(byte[] data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void pushData(List<byte[]> data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ITuple pullDataItems(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

}
