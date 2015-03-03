package uk.ac.imperial.lsds.seepworker.core.input;

import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import static com.codahale.metrics.MetricRegistry.name;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.metrics.SeepMetrics;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class NetworkDataStream implements InputAdapter{

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	final private DataStoreType TYPE = DataStoreType.NETWORK;
	
	private InputBuffer buffer;
	private BlockingQueue<byte[]> queue;
	private int queueSize;
	
	final private List<Integer> representedIds;
	final private int streamId;
	private ITuple iTuple;
	
	// Metrics
	final Counter qSize;
	
	public NetworkDataStream(WorkerConfig wc, int opId, int streamId, Schema expectedSchema) {
		this.representedIds = new ArrayList<>();
		this.representedIds.add(opId);
		this.streamId = streamId;
		this.iTuple = new ITuple(expectedSchema);
		this.queueSize = wc.getInt(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH);
		this.queue = new ArrayBlockingQueue<byte[]>(queueSize);
		int headroom = wc.getInt(WorkerConfig.BATCH_SIZE) * 2;
		this.buffer = new InputBuffer(headroom);
		qSize = SeepMetrics.REG.counter(name(NetworkDataStream.class, "queue", "size"));
	}
	
	@Override
	public DataStoreType getDataOriginType() {
		return TYPE;
	}

	@Override
	public short returnType() {
		return RETURN_TYPE;
	}
	
	public List<Integer> getRepresentedOpId(){
		return representedIds;
	}
	
	@Override
	public int getStreamId(){
		return streamId;
	}
		
	@Override
	public void readFrom(ReadableByteChannel channel, int id) {
		// TODO: note id is still useful here for metrics
		buffer.readFrom(channel, this);
	}
	
	@Override
	public void pushData(byte[] data){
		try {
			queue.put(data);
			qSize.inc();
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public ITuple pullDataItem(int timeout) {
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
		qSize.dec(); // decrement only when is not null
		iTuple.setData(data);
		iTuple.setStreamId(streamId);
		return iTuple;
	}

	@Override
	public ITuple pullDataItems(int timeout) {
		// TODO batching oriented, or window, or barrier, etc...
		return null;
	}
	
	@Override
	public void pushData(List<byte[]> data) {
		// TODO Auto-generated method stub
		
	}
}
