package uk.ac.imperial.lsds.seepworker.core.input;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;
import uk.ac.imperial.lsds.seep.metrics.SeepMetrics;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

import com.codahale.metrics.Counter;

public class NetworkDataStream implements InputAdapter {

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	final private DataStoreType TYPE = DataStoreType.NETWORK;
	
	private int streamId;
	private IBuffer buffer;
	private ITuple iTuple;
	
	// Metrics
	final Counter qSize;
	
	public NetworkDataStream(WorkerConfig wc, int streamId, InputBuffer buffer, Schema expectedSchema) {
		this.streamId = streamId;
		this.buffer = buffer;
		this.iTuple = new ITuple(expectedSchema);
		qSize = SeepMetrics.REG.counter(name(NetworkDataStream.class, "queue", "size"));
	}

	@Override
	public int getStreamId() {
		return streamId;
	}
	
	@Override
	public DataStoreType getDataStoreType() {
		return TYPE;
	}

	@Override
	public short returnType() {
		return RETURN_TYPE;
	}
	
	@Override
	public ITuple pullDataItem(int timeout) {
		byte[] data = buffer.read(timeout);
		if(data == null) {
			return null;
		}
		iTuple.setData(data);
		iTuple.setStreamId(streamId);
		return iTuple;
	}

	@Override
	public List<ITuple> pullDataItems(int timeout) {
		// TODO Auto-generated method stub
		return null;
	}

}
