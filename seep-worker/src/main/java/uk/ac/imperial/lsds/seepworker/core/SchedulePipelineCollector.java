package uk.ac.imperial.lsds.seepworker.core;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;

public class SchedulePipelineCollector implements API {

	public SchedulePipelineCollector(int id, CoreOutput coreOutput, SeepTask task) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int id() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void send(byte[] o) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		return null;
	}

}
