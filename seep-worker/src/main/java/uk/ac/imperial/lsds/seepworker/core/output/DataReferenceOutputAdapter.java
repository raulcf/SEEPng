package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class DataReferenceOutputAdapter implements OutputAdapter {

	public DataReferenceOutputAdapter(WorkerConfig wc, int streamId, Set<DataReference> value) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void send(byte[] o) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		return 0;
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<DataReference> getOutputDataReference() {
		// TODO Auto-generated method stub
		return null;
	}

}
