package uk.ac.imperial.lsds.seepworker.core.input;

import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class NetworkBarrier implements InputAdapter {

	final private short RETURN_TYPE = InputAdapterReturnType.MANY.ofType();
	final private DataStoreType TYPE = DataStoreType.NETWORK;
	
	private int streamId;
	private ITuple iTuple;
	private BlockingQueue<List<byte[]>> queue;
	private int queueSize;
	
	private Set<Integer> barrierMembers;
	private Set<Integer> membersArrivedInBarrier;
	private Map<Integer, InputBuffer> inputBuffers;
	private List<byte[]> data;
	
	public NetworkBarrier(WorkerConfig wc, int streamId, Schema expectedSchema, List<UpstreamConnection> upc) {
		this.streamId = streamId;
		this.iTuple = new ITuple(expectedSchema);
		this.queueSize = wc.getInt(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH);
		this.queue = new ArrayBlockingQueue<>(queueSize);
		this.membersArrivedInBarrier = new HashSet<>();
		this.barrierMembers = new HashSet<>();
		int headroom = wc.getInt(WorkerConfig.BATCH_SIZE) * 2;
		this.inputBuffers = new HashMap<>();
		for(UpstreamConnection uc : upc){
			int id = uc.getUpstreamOperator().getOperatorId();
			barrierMembers.add(id);
			inputBuffers.put(id, new InputBuffer(headroom));
		}
		this.data = new ArrayList<>();
	}

	@Override
	public List<Integer> getRepresentedOpId(){
		return new ArrayList<Integer>(barrierMembers);
	}
	
	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public short returnType() {
		return RETURN_TYPE;
	}
	
	@Override
	public DataStoreType getDataOriginType() {
		return TYPE;
	}
	
	@Override
	public void readFrom(ReadableByteChannel channel, int id) {
		if(! membersArrivedInBarrier.contains(id)){
			// read data from channel and push it to the barrierBatch
			InputBuffer buffer = inputBuffers.get(id);
			buffer.readToInternalBuffer(channel, this);
			// if data was fully read, then add member to arrived set
			if(buffer.hasCompletedReads()) {
				membersArrivedInBarrier.add(id);
				this.data.add(buffer.read());
				
				// Check whether all members already arrived
				if(membersArrivedInBarrier.containsAll(barrierMembers)) {
					// clean appropiately the structures and make data available for consumption
					// TODO: can avoid this copy?
					List<byte[]> copy = new ArrayList<>(data);
					this.pushData(copy);
					data.clear();
					membersArrivedInBarrier.clear();
				}
			}
		}
	}
	
	@Override
	public void pushData(List<byte[]> data) {
		try {
			queue.put(data);
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public ITuple pullDataItems(int timeout) {
		List<byte[]> data = null;
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
	public void pushData(byte[] data) {
		// TODO non-defined
	}

	@Override
	public ITuple pullDataItem(int timeout) {
		// TODO non-defined
		return null;
	}
}
