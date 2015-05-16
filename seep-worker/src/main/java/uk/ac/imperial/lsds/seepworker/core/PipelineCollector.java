package uk.ac.imperial.lsds.seepworker.core;

import java.util.Deque;
import java.util.Iterator;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;

public class PipelineCollector implements API {

	private Deque<SeepTask> tasks;
	private Iterator<SeepTask> taskIterator;
	private byte[] data;
	
	public PipelineCollector(Deque<SeepTask> tasks) {
		this.tasks = tasks;
		this.taskIterator = tasks.iterator();
	}
	
	public byte[] collect(){
		taskIterator = tasks.iterator();
		return data;
	}

	@Override
	public int id() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void send(byte[] o) {
		// somehow build a ituple from o
		if(taskIterator.hasNext()) {
			taskIterator.next().processData(null, this);
		}
		else{
			this.data = o;
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
