package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;


public class SimpleNetworkOutput implements OutputAdapter {

	private final boolean SINGLE_SEND_NOT_DEFINED;
	
	final private DataStoreType TYPE = DataStoreType.NETWORK;
	
	private int streamId;
	private Router router;
	private Map<Integer, OutputBuffer> outputBuffers;
	private OutputBuffer ob;
	private EventAPI eAPI;
	
	public SimpleNetworkOutput(int streamId, Router router, Map<Integer, OutputBuffer> outputBuffers){
		this.router = router;
		this.streamId = streamId;
		this.outputBuffers = outputBuffers;
		if(outputBuffers.size() == 1){
			SINGLE_SEND_NOT_DEFINED = false;
			ob = outputBuffers.values().iterator().next();
		}
		else{
			SINGLE_SEND_NOT_DEFINED = true;
		}
	}
	
	@Override
	public void setEventAPI(EventAPI eAPI) {
		this.eAPI = eAPI;
	}
	
	@Override
	public DataStoreType getDataOriginType() {
		return TYPE;
	}
	
	@Override
	public Map<Integer, OutputBuffer> getOutputBuffers(){
		return outputBuffers;
	}

	@Override
	public int getStreamId() {
		return streamId;
	}

	@Override
	public void send(byte[] o) {
		OutputBuffer outB = ob;
		if(SINGLE_SEND_NOT_DEFINED){
			int opId = this.router.route();
			outB = outputBuffers.get(opId);
		}
		boolean completed = outB.write(o);
		if(completed){
			eAPI.readyForWrite(outB.id());
		}
	}

	@Override
	public void sendAll(byte[] o) {
		List<Integer> ids = new ArrayList<>();
		for(OutputBuffer ob : outputBuffers.values()){
			boolean completed = ob.write(o);
			if(completed){
				ids.add(ob.id());
			}
		}
		if(ids.size() > 0){
			eAPI.readyForWrite(ids);
		}
	}

	@Override
	public void sendKey(byte[] o, int key) {
		int opId = router.route(key);
		OutputBuffer ob = outputBuffers.get(opId);
		
		boolean complete = ob.write(o);
		if(complete){
			eAPI.readyForWrite(ob.id());
		}
	}

	/**
	 * TODO: fix these non-defined things
	 */
	
	@Override
	public void sendKey(byte[] o, String key) {
		// NON DEFINED
	}

	@Override
	public void sendToStreamId(int streamId, byte[] o) {
		// NON DEFINED
	}

	@Override
	public void sendToAllInStreamId(int streamId, byte[] o) {
		// NON DEFINED
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, int key) {
		// NON DEFINED
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, String key) {
		// NON DEFINED
	}

	@Override
	public void send_index(int index, byte[] o) {
		// TODO Auto-generated method stub
		// careful i guess
	}

	@Override
	public void send_opid(int opId, byte[] o) {
		// TODO Auto-generated method stub
		// careful again
	}

}

