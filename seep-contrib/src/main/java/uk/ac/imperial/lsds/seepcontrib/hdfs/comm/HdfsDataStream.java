package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.net.SocketInputStream;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seepcontrib.hdfs.config.HdfsConfig;

public class HdfsDataStream implements InputAdapter {
	
	final private DataStoreType TYPE = DataStoreType.HDFS;

	final private int streamId;
	private ITuple iTuple;
	final private List<Integer> representedIds;
	private int queueSize;
	private SocketInputStream si;
	private int timeout;

	private BlockingQueue<byte[]> queue;
	
	public HdfsDataStream(int opId, int streamId, Schema expectedSchema) {
		this.representedIds = new ArrayList<>();
		this.representedIds.add(opId);
		this.streamId = streamId;
		this.iTuple = new ITuple(expectedSchema);
		this.queueSize = HdfsConfig.INPUT_QUEUE_SIZE;
		this.timeout = HdfsConfig.READING_TIME_OUT;
		this.queue = new ArrayBlockingQueue<byte[]>(queueSize);
	}

	@Override
	public List<Integer> getRepresentedOpId() {
		return representedIds;
	}

	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public short returnType() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public DataStoreType getDataOriginType() {
		return TYPE;
	}

	@Override
	public void readFrom(ReadableByteChannel channel, int id) {
		si = new SocketInputStream(channel,timeout);
		byte[] buffer;
		si.read(buffer);
		this.pushData(buffer);
	}

	@Override
	public void pushData(byte[] data) {
		try {
			queue.put(data);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		byte[] data = null;
		try {
			if(timeout > 0){
				data = queue.poll(timeout, TimeUnit.MILLISECONDS);
			} else{
				data = queue.take();
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		// In case poll was used, and it timeouts
		if(data == null){
			return null;
		}
		iTuple.setData(data);
		iTuple.setStreamId(streamId);
		return iTuple;
	}

	@Override
	public ITuple pullDataItems(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pushData(List<byte[]> data) {
		// TODO Auto-generated method stub
	}
}
