package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Dataset;

public class DatasetInputAdapter implements InputAdapter {

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	
	private int streamId;
	private Dataset dataset;
	private ITuple iTuple;
	
	public DatasetInputAdapter(WorkerConfig wc, int streamId, Dataset dataset) {
		this.streamId = streamId;
		this.dataset = dataset;
		Schema expectedSchema = this.dataset.getSchemaForDataset();
		this.iTuple = new ITuple(expectedSchema);
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
	public DataStoreType getDataStoreType() {
		// In this case it's dynamic.
		return dataset.getDataReference().getDataStore().type();
	}
	
	@Override
	public ITuple pullDataItem(int timeout) {
		byte[] data = dataset.consumeData();
		iTuple.setData(data);
		iTuple.setStreamId(streamId);
		return iTuple;
	}

	@Override
	public List<ITuple> pullDataItems(int timeout) {
		// TODO: will use Dataset.read(int number elements);
		return null;
	}

}
