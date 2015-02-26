package uk.ac.imperial.lsds.seep.api.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RowBatchITuple implements DataItem {

	private final List<byte[]> batch;
	private final Iterator<byte[]> it;
	private final ITuple iTuple;
	private final int streamId;
	
	private RowBatchITuple(RowBatchITupleBuilder builder){
		batch = new ArrayList<>(builder.batch);
		it = batch.iterator();
		this.iTuple = builder.iTuple;
		this.streamId = builder.streamId;
	}
	
	@Override
	public ITuple consume() {
		if(it.hasNext()) {
			byte[] raw = it.next();
			iTuple.setData(raw);
			iTuple.setStreamId(streamId);
			return iTuple;
		}
		return null;
	}
	
	public static class RowBatchITupleBuilder{
		
		private ITuple iTuple;
		private int streamId;
		
		private final int BATCH_SIZE;
		private List<byte[]> batch;
		
		public RowBatchITupleBuilder(int batchSize, ITuple iTuple, int streamId){
			this.iTuple = iTuple;
			this.streamId = streamId;
			this.BATCH_SIZE = batchSize;
			this.batch = new ArrayList<>(batchSize);
		}
		
		public boolean add(byte[] data) {
			this.batch.add(data);
			return (batch.size() == BATCH_SIZE);
		}
		
		public RowBatchITuple build(){
			return new RowBatchITuple(this);
		}
		
		public void reset(){
			this.batch.clear();
		}
		
	}

}
