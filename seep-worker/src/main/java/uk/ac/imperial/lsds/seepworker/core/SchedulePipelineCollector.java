package uk.ac.imperial.lsds.seepworker.core;

import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;

public class SchedulePipelineCollector implements API {

	private Deque<LogicalOperator> operators;
	private Iterator<LogicalOperator> opIterator;
	private List<OutputAdapter> outputAdapters;
	
	
	public SchedulePipelineCollector(Deque<LogicalOperator> operators, List<OutputAdapter> outputAdapters) {
		this.operators = operators;
		this.opIterator = operators.iterator();
		this.outputAdapters = outputAdapters;
	}
	
	public void rewindPipeline(){
		opIterator = operators.iterator();
	}

	@Override
	public int id() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public void processData(ITuple iTuple) {
		opIterator.next().getSeepTask().processData(iTuple, this);
	}

	@Override
	public void send(byte[] o) {
		// somehow build a ituple from o
		if(opIterator.hasNext()) {
			// FIXME: clearly avoid serde here
			LogicalOperator next = opIterator.next();
			Schema schema = next.upstreamConnections().get(0).getExpectedSchema(); // 0 cause there's only 1
			ITuple nextTuple = new ITuple(schema);
			nextTuple.setData(o);
			next.getSeepTask().processData(nextTuple, this);
		}
		else{
			// FIXME: don't write there, instead send to the 
//			this.data = o;
			return;
		}
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
}
