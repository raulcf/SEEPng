package uk.ac.imperial.lsds.seepworker.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.errors.DoYouKnowWhatYouAreDoingException;
import uk.ac.imperial.lsds.seepworker.core.output.routing.NotEnoughRoutingInformation;

public class Collector implements API {

	private final boolean NOT_SEND_API;
	private final boolean SINGLE_SEND_NOT_DEFINED;
	
	private int id;
	private OutputAdapter outputAdapter;
	private List<OutputAdapter> outputAdapters;
	private Map<Integer, OutputAdapter> streamIdToOutputAdapter;
	
	public Collector(int id, List<OutputAdapter> outputAdapters){
		this.id = id;
		int numOutputAdapters = outputAdapters.size();
		if(numOutputAdapters > 0){
			NOT_SEND_API = false;
			if(numOutputAdapters == 1){
				SINGLE_SEND_NOT_DEFINED = false;
				// Configure the unique outputadapter in this collector to avoid one lookup
				outputAdapter = outputAdapters.get(0);
				//************************************
				this.outputAdapters = outputAdapters;
				this.streamIdToOutputAdapter = createMap(outputAdapters);
				//************************************
			}
			else{
				SINGLE_SEND_NOT_DEFINED = true;
				this.outputAdapters = outputAdapters;
				this.streamIdToOutputAdapter = createMap(outputAdapters);
			}	
		}
		else if(numOutputAdapters == 0){
			NOT_SEND_API = true;
			SINGLE_SEND_NOT_DEFINED = true;
		}
		else{
			NOT_SEND_API = true;
			SINGLE_SEND_NOT_DEFINED = true;
		}
	}
	
	private Map<Integer, OutputAdapter> createMap(List<OutputAdapter> outputAdapters){
		Map<Integer, OutputAdapter> tr = new HashMap<>();
		for(OutputAdapter o : outputAdapters){
			tr.put(o.getStreamId(), o);
		}
		return tr;
	}
	
	@Override
	public int id(){
		return id;
	}
	
	@Override
	public void send(byte[] o) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		if(SINGLE_SEND_NOT_DEFINED){
			throw new NotEnoughRoutingInformation("There are more than one streamId downstream; you must specify where "
					+ "you are sending to");
		}
		outputAdapter.send(o);
	}

	@Override
	public void sendAll(byte[] o) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		if(SINGLE_SEND_NOT_DEFINED){
			throw new NotEnoughRoutingInformation("There are more than one streamId downstream; you must specify where "
					+ "you are sending to");
		}
		outputAdapter.sendAll(o);
	}

	@Override
	public void sendKey(byte[] o, int key) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		outputAdapter.sendKey(o, key);
	}

	@Override
	public void sendKey(byte[] o, String key) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		outputAdapter.sendKey(o, key);
	}

	@Override
	public void sendToStreamId(int streamId, byte[] o) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		streamIdToOutputAdapter.get(streamId).send(o);
	}

	@Override
	public void sendToAllInStreamId(int streamId, byte[] o) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		streamIdToOutputAdapter.get(streamId).sendAll(o);
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, int key) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		streamIdToOutputAdapter.get(streamId).sendKey(o, key);
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, String key) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		streamIdToOutputAdapter.get(streamId).sendKey(o, key);
	}

	@Override
	public void send_index(int index, byte[] o) {
		throw new DoYouKnowWhatYouAreDoingException("This is mostly a debugging method, you should not be playing with the"
				+ "underlying communication directly otherwise...");
	}

	@Override
	public void send_opid(int opId, byte[] o) {
		throw new DoYouKnowWhatYouAreDoingException("You seem to know too much about the topology of this dataflow...");
	}
	
}
