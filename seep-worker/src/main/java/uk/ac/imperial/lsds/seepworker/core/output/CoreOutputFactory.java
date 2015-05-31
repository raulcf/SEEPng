package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.core.OBuffer;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.DataReferenceManager;

public class CoreOutputFactory {

	final private static Logger LOG = LoggerFactory.getLogger(CoreOutputFactory.class);
	
//	@Deprecated
//	public static CoreOutput buildCoreOutputForOperator(WorkerConfig wc, LogicalOperator o, Map<Integer, EndPoint> mapping){
//		LOG.info("Building coreOutput...");
//		List<OutputAdapter> outputAdapters = new ArrayList<>();
//		// Create an OutputAdapter per downstream connection -> know with the streamId
//		Map<Integer, List<DownstreamConnection>> streamToOpConn = new HashMap<>();
//		for(DownstreamConnection dc : o.downstreamConnections()){
//			int streamId = dc.getStreamId();
//			if(streamToOpConn.containsKey(streamId)){
//				streamToOpConn.get(streamId).add(dc);
//			}
//			else{
//				List<DownstreamConnection> l = new ArrayList<>();
//				l.add(dc);
//				streamToOpConn.put(streamId, l);
//			}
//		}
//		// Perform sanity check. All ops for a given streamId should have same schema
//		// TODO:
//		
//		// Build an output adapter per streamId
//		for(Integer streamId : streamToOpConn.keySet()){
//			
//			List<DownstreamConnection> doCon = streamToOpConn.get(streamId);
//			DataStoreType dOriginType = doCon.get(0).getExpectedDataStoreTypeOfDownstream();
//			
//			OutputAdapter oa = null;
//			if(dOriginType == DataStoreType.NETWORK){
//				// Create outputAdapter
//				LOG.info("Building outputAdapter for downstream streamId: {} of type: {}", streamId, "NETWORK");
//				oa = OutputAdapterFactory.buildOutputAdapterOfTypeNetworkForOps(wc, streamId, doCon, mapping);
//			}
//			else if(dOriginType == DataStoreType.KAFKA){
//				// Create outputAdapter to send data to Kafka, and *not* to the downstream operator
//				KafkaConfig kc = new KafkaConfig( doCon.get(0).getExpectedDataStoreOfDownstream().getConfig() );
//				LOG.info("Building outputAdapter for downstream streamId: {} of type: {}", streamId, "KAFKA");
//				oa = OutputAdapterFactory.buildOutputAdapterOfTypeKafkaForOps(kc, streamId, doCon);
//			}
//			outputAdapters.add(oa);
//		}
//		CoreOutput cOutput = new CoreOutput(outputAdapters);
//		LOG.info("Building coreOutput...OK");
//		return cOutput;
//	}
	
	public static CoreOutput buildCoreOutputFor(WorkerConfig wc, DataReferenceManager drm, Map<Integer, Set<DataReference>> output) {
		LOG.info("Building coreOutput...");
		Map<Integer, OBuffer> oBuffers = new HashMap<>();
		Map<Integer, List<OBuffer>> streamId_To_OBuffers = new HashMap<>();
		for(Entry<Integer, Set<DataReference>> entry : output.entrySet()) {
			int streamId = entry.getKey();
			List<OBuffer> buffers = new ArrayList<>();
			for(DataReference dr : entry.getValue()) {
				drm.manageNewDataReference(dr);
				OBuffer ob = new OutputBuffer(dr, wc.getInt(WorkerConfig.BATCH_SIZE));
				oBuffers.put(dr.getId(), ob); // dr.id -> obuffer
				buffers.add(ob);
			}
			streamId_To_OBuffers.put(streamId, buffers);
		}
		CoreOutput cOutput = new CoreOutput(output, streamId_To_OBuffers, oBuffers);
		LOG.info("Building coreOutput...OK");
		return cOutput;
	}

	public static CoreOutput buildCoreOutputForStage(WorkerConfig wc, Map<Integer, Set<DataReference>> output) {
//		LOG.info("Building coreOutput...");
//		List<OutputAdapter> outputAdapters = new ArrayList<>();
//		// we are already given the downstream streamId
//		for(Entry<Integer, Set<DataReference>> entry : output.entrySet()){
//			OutputAdapter oa = OutputAdapterFactory.buildOutputAdapterForDataReference(wc, entry.getKey(), entry.getValue());
//			outputAdapters.add(oa);
//		}
//		CoreOutput cOutput = new CoreOutput(outputAdapters);
//		LOG.info("Building coreOutput...OK");
//		return cOutput;
		return null;
	}
}
