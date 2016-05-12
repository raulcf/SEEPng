package uk.ac.imperial.lsds.seepworker.core.input;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.ZCITuple;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.InputAdapterReturnType;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.Dataset;

public class DatasetInputAdapter implements InputAdapter {

	final private short RETURN_TYPE = InputAdapterReturnType.ONE.ofType();
	
	private int streamId;
	private Dataset dataset;
	private ZCITuple iTuple;
	
	public DatasetInputAdapter(WorkerConfig wc, int streamId, Dataset dataset) {
		this.streamId = streamId;
		this.dataset = dataset;
		Schema expectedSchema = this.dataset.getSchemaForDataset();
		//this.iTuple = new ITuple(expectedSchema);
		this.iTuple = new ZCITuple(expectedSchema);
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
		ITuple i = dataset.consumeData_zerocopy(iTuple);
		return i;
//		byte[] data = dataset.consumeData();
//		if(data == null) return null;
//		iTuple.setData(data);
//		iTuple.setStreamId(streamId);
//		return iTuple;
	}
	
	public ITuple _pullDataItem(int timeout) {
		byte[] data = dataset.consumeData();
		if(data == null) return null;
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
