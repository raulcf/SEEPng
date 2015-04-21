package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.DataItem;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.InputAdapter;

public class HdfsInputAdapter implements InputAdapter {
	
	final private DataStoreType TYPE = DataStoreType.HDFS;
	
	final private int streamId;
	private ITuple iTuple;
	final private List<Integer> representedIds;

	private BlockingQueue<byte[]> queue;
	private HdfsInputBuffer buffer;

	private int queueSize;
	
	public HdfsInputAdapter(int opId, int streamId, Schema expectedSchema, int queuelength,int headroom) {
		
		this.representedIds = new ArrayList<>();
		this.representedIds.add(opId);
		this.streamId = streamId;
		this.iTuple = new ITuple(expectedSchema);
		this.queue = new ArrayBlockingQueue<byte[]>(queuelength);
		this.buffer = new HdfsInputBuffer(headroom);
	}
	@Override
	public List<Integer> getRepresentedOpId() {
		// TODO Auto-generated method stub
		return representedIds;
	}

	@Override
	public int getStreamId() {
		// TODO Auto-generated method stub
		return streamId;
	}

	@Override
	public short returnType() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DataStoreType getDataOriginType() {
		// TODO Auto-generated method stub
		return TYPE;
	}

	@Override
	public void readFrom(ReadableByteChannel channel, int id) {
		buffer.readFrom(channel, this);
	}

	@Override
	public void pushData(byte[] data) {
		try {
			queue.put(data);
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void pushData(List<byte[]> data) {
		// TODO Auto-generated method stub

	}

	@Override
	public DataItem pullDataItem(int timeout) {
		byte[] data = null;
		try {
			if(timeout > 0){
				// Need to poll rather than take due to the implementation of some ProcessingEngines
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
	public DataItem pullDataItems(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

}