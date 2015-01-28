package uk.ac.imperial.lsds.seep.api;


public interface CommAPI {
	
	public void send(byte[] o);
	public void sendAll(byte[] o);
	public void sendKey(byte[] o, int key);
	public void sendKey(byte[] o, String key);
	public void sendToStreamId(int streamId, byte[] o);
	public void sendToAllInStreamId(int streamId, byte[] o);
	public void sendStreamidKey(int streamId, byte[] o, int key);
	public void sendStreamidKey(int streamId, byte[] o, String key);
	public void send_index(int index, byte[] o);
	public void send_opid(int opId, byte[] o);
	
}
