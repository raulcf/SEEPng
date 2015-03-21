package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsOutputAdapter implements OutputAdapter {
	final private DataStoreType TYPE = DataStoreType.HDFS;
	private  FSDataOutputStream outputStream;
	

	public HdfsOutputAdapter(FileSystem fs, Path f) throws IOException {
		outputStream = fs.create(f);
		
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
		return TYPE;
	}

}
