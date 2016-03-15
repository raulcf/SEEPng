package uk.ac.imperial.lsds.seepworker.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.RuntimeEvent;
import uk.ac.imperial.lsds.seep.api.RuntimeEventFactory;
import uk.ac.imperial.lsds.seep.core.EventBasedOBuffer;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.errors.DoYouKnowWhatYouAreDoingException;
import uk.ac.imperial.lsds.seepworker.core.output.CoreOutput;
import uk.ac.imperial.lsds.seepworker.core.output.routing.NotEnoughRoutingInformation;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;
import uk.ac.imperial.lsds.seepworker.core.output.routing.RouterFactory;

public class Collector implements API {

	// Attributes for CommAPI
	private final boolean NOT_SEND_API;
	private final boolean SINGLE_SEND_NOT_DEFINED;
	private final boolean MULTIPLE_STREAMID;
	
	final private int id;
	private Map<Integer, List<OBuffer>> streamId_To_OBuffer;
	private Map<Integer, OBuffer> buffers;
	
	private Map<Integer, Router> streamId_To_Router;
	
	private Router theRouter;
	private OBuffer theOBuffer;
	
	// Attributes for RuntimeEvent
	private List<RuntimeEvent> rEvents;
	
	public Collector(int id, CoreOutput coreOutput) {
		this.rEvents = new ArrayList<>();
		
		this.id = id;
		this.streamId_To_OBuffer = coreOutput.getStreamIdToBuffers();
		this.buffers = coreOutput.getBuffers();
		
		int numOutputBuffers = buffers.size();
		int logicalDownstreams = streamId_To_OBuffer.size();
		
		if(numOutputBuffers == 0) {
			NOT_SEND_API = true;
			SINGLE_SEND_NOT_DEFINED = true;
			MULTIPLE_STREAMID = false;
		}
		else {
			NOT_SEND_API = false; // we can send
			if(logicalDownstreams == 1) { // only one streamId
				MULTIPLE_STREAMID = false;
				if(numOutputBuffers == 1) { // only one physical downstream -> 1 buffer
					SINGLE_SEND_NOT_DEFINED = false; // we can send to only one
					theOBuffer = buffers.values().iterator().next();
				}
				else { // multiple physical downstreams of the same streamId
					SINGLE_SEND_NOT_DEFINED = true;
					// TODO: create one router
					List<Integer> oBufferIds = getBufferIds(buffers.values());
					boolean stateful = buffers.values().iterator().next().getDataReference().isPartitioned();
					theRouter = RouterFactory.buildRouterFor(oBufferIds, stateful);
				}
			}
			else { // multiple streamId
				MULTIPLE_STREAMID = true;
				SINGLE_SEND_NOT_DEFINED = true;
				streamId_To_Router = createRouters(streamId_To_OBuffer);
			}
		}
	}
	
	private List<Integer> getBufferIds(Collection<OBuffer> collection) {
		List<Integer> ids = new ArrayList<>();
		for(OBuffer buffer : collection) {
			ids.add(buffer.getDataReference().getId());
		}
		return ids;
	}
	
	private Map<Integer, Router> createRouters(Map<Integer, List<OBuffer>> streamId_To_OBuffer) {
		Map<Integer, Router> routers = new HashMap<>();
		for(Entry<Integer, List<OBuffer>> entry : streamId_To_OBuffer.entrySet()) {
			int streamId = entry.getKey();
			List<Integer> ids = getBufferIds(entry.getValue());
			boolean stateful = entry.getValue().iterator().next().getDataReference().isPartitioned();
			Router r = RouterFactory.buildRouterFor(ids, stateful);
			routers.put(streamId, r);
		}
		return routers;
	}

	@Override
	public int id() {
		return id;
	}

	@Override
	public void send(byte[] o) {
		OBuffer ob = theOBuffer;
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		if(MULTIPLE_STREAMID) {
			throw new NotEnoughRoutingInformation("There are more than one streamId downstream; you must specify where "
					+ "you are sending to");
		}
		if(SINGLE_SEND_NOT_DEFINED) {
			int id = theRouter.route();
			ob = buffers.get(id);
		}
		boolean completed = ob.write(o, this);
		if(completed && ob instanceof EventBasedOBuffer) {
			((EventBasedOBuffer)ob).getEventAPI().readyForWrite(id);
		}
	}
	
	@Override
	public void sendKey(byte[] o, int key) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		int id = theRouter.route(key);
		OBuffer ob = buffers.get(id);
		
		boolean completed = ob.write(o, this);
		if(completed && ob instanceof EventBasedOBuffer) {
			((EventBasedOBuffer)ob).getEventAPI().readyForWrite(id);
		}
	}

	@Override
	public void sendAll(byte[] o) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		if(MULTIPLE_STREAMID) {
			throw new NotEnoughRoutingInformation("There are more than one streamId downstream; you must specify where "
					+ "you are sending to");
		}
		// Regardless multiple_send or not
		List<Integer> ids = new ArrayList<>();
		OBuffer ob = null;
		for(Entry<Integer, OBuffer> entry : buffers.entrySet()){
			int id = entry.getKey();
			ob = entry.getValue();
			boolean completed = ob.write(o, this);
			if(completed && ob instanceof EventBasedOBuffer) {
				ids.add(id);
			}
		}
		if(ids.size() > 0){
			((EventBasedOBuffer)ob).getEventAPI().readyForWrite(ids);
		}
	}

	@Override
	public void sendKey(byte[] o, String key) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		int hashKey = key.hashCode();
		this.sendKey(o, hashKey);
	}

	@Override
	public void sendToStreamId(int streamId, byte[] o) {
		if(NOT_SEND_API) throw new UnsupportedOperationException("Send API not defined, maybe this is a sink?");
		if(! MULTIPLE_STREAMID) throw new UnsupportedOperationException("Only one streamId available!");
		Router r = streamId_To_Router.get(streamId);
		int id = r.route();
		OBuffer ob = buffers.get(id);
		boolean completed = ob.write(o, this);
		if(completed && ob instanceof EventBasedOBuffer) {
			((EventBasedOBuffer)ob).getEventAPI().readyForWrite(id);
		}
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
		throw new DoYouKnowWhatYouAreDoingException("This is mostly a debugging method, you should not be playing with the"
				+ "underlying communication directly otherwise...");
	}

	@Override
	public void send_opid(int opId, byte[] o) {
		throw new DoYouKnowWhatYouAreDoingException("You seem to know too much about the topology of this dataflow...");
	}
	
	/**
	 * Implement RuntimeEvent interface
	 */
	
	@Override
	public List<RuntimeEvent> getRuntimeEvents() {
		return rEvents;
	}

	@Override
	public void exception(String message) {
		// TODO: implement
		try {
			throw new Exception();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void datasetSpilledToDisk(int datasetId) {
		RuntimeEvent re = RuntimeEventFactory.makeSpillToDiskRuntimeEvent(datasetId);
		this.rEvents.add(re);
	}

	@Override
	public void failure() {
		// TODO: implement
		try {
			throw new Exception();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void notifyEndOfLoop() {
		RuntimeEvent re = RuntimeEventFactory.makeNotifyEndOfLoop();
		this.rEvents.add(re);
	}

	@Override
	public void storeEvaluateResults(Object obj) {
		RuntimeEvent re = RuntimeEventFactory.makeEvaluateResults(obj);
		this.rEvents.add(re);
		
	}

	

}
