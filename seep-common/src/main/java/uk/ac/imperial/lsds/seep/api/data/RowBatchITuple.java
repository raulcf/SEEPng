package uk.ac.imperial.lsds.seep.api.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RowBatchITuple implements DataItem {

	private List<byte[]> batch;
	private Iterator<byte[]> it;
	private ITuple iTuple;
	private int streamId;
	
	public RowBatchITuple(int size, ITuple iTuple, int streamId){
		batch = new ArrayList<>(size);
		this.iTuple = iTuple;
		this.streamId = streamId;
	}
	
	public void add(byte[] data){
		this.batch.add(data);
	}
	
	public void close(){
		it = batch.iterator();
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

}
