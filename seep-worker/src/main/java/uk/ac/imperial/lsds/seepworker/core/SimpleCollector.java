package uk.ac.imperial.lsds.seepworker.core;

import uk.ac.imperial.lsds.seep.api.API;

public class SimpleCollector implements API {

	private byte[] mem;
	
	public SimpleCollector() { }
	
	@Override
	public int id() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void send(byte[] o) {
		this.mem = o;
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
	
	public byte[] collect() {
		return mem;
	}

}
