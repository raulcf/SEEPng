package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.operator.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepcontrib.kafka.config.KafkaConfig;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.input.CoreInput;
import uk.ac.imperial.lsds.seepworker.core.input.InputAdapterFactory;

public class CoreOutputFactory {

	final private static Logger LOG = LoggerFactory.getLogger(CoreOutputFactory.class);
	
	public static CoreOutput buildCoreOutputForOperator(WorkerConfig wc, LogicalOperator o, Map<Integer, EndPoint> mapping){
		LOG.info("Building coreOutput...");
		List<OutputAdapter> outputAdapters = new ArrayList<>();
		// Create an OutputAdapter per downstream connection -> know with the streamId
		Map<Integer, List<DownstreamConnection>> streamToOpConn = new HashMap<>();
		for(DownstreamConnection dc : o.downstreamConnections()){
			int streamId = dc.getStreamId();
			if(streamToOpConn.containsKey(streamId)){
				streamToOpConn.get(streamId).add(dc);
			}
			else{
				List<DownstreamConnection> l = new ArrayList<>();
				l.add(dc);
				streamToOpConn.put(streamId, l);
			}
		}
		// Perform sanity check. All ops for a given streamId should have same schema
		// TODO:
		
		// Build an output adapter per streamId
		for(Integer streamId : streamToOpConn.keySet()){
			
			List<DownstreamConnection> doCon = streamToOpConn.get(streamId);
			DataStoreType dOriginType = doCon.get(0).getExpectedDataStoreTypeOfDownstream();
			
			OutputAdapter oa = null;
			if(dOriginType == DataStoreType.NETWORK){
				// Create outputAdapter
				LOG.info("Building outputAdapter for downstream streamId: {} of type: {}", streamId, "NETWORK");
				oa = OutputAdapterFactory.buildOutputAdapterOfTypeNetworkForOps(wc, streamId, doCon, mapping);
			}
			else if(dOriginType == DataStoreType.KAFKA){
				// Create outputAdapter to send data to Kafka, and *not* to the downstream operator
				KafkaConfig kc = new KafkaConfig( doCon.get(0).getExpectedDataStoreOfDownstream().getConfig() );
				LOG.info("Building outputAdapter for downstream streamId: {} of type: {}", streamId, "KAFKA");
				oa = OutputAdapterFactory.buildOutputAdapterOfTypeKafkaForOps(kc, streamId, doCon);
			}
			outputAdapters.add(oa);
		}
		CoreOutput cOutput = new CoreOutput(outputAdapters);
		LOG.info("Building coreOutput...OK");
		return cOutput;
	}
	
//	LOG.info("Building Core Input...");
//	List<InputAdapter> inputAdapters = new LinkedList<>();
//	for(Entry<Integer, Set<DataReference>> entry : input.entrySet()) {
//		int streamId = entry.getKey();
//		Set<DataReference> drefs = entry.getValue();
//		List<InputAdapter> ias = InputAdapterFactory.buildInputAdapterForStreamId(wc, streamId, drefs, connTypeInformation.get(streamId));
//		if(ias != null){
//			inputAdapters.addAll(ias);
//		}
//	}
//	CoreInput ci = new CoreInput(inputAdapters);
//	LOG.info("Building Core Input...OK");
//	return ci;
	
	public static CoreOutput buildCoreOutputFor(WorkerConfig wc, Map<Integer, Set<DataReference>> output) {
		for(Entry<Integer, Set<DataReference>> entry : output.entrySet()) {
			int streamId = entry.getKey();
			if(! output.get(streamId).isEmpty()) {
				// In this case register DataReferences so that they can be consumed by anyone
			}
			else {
				// In this case create new DataReferences
			}
		}
		// TODO Auto-generated method stub
		return null;
	}

	public static CoreOutput buildCoreOutputForStage(WorkerConfig wc, Map<Integer, Set<DataReference>> output) {
		LOG.info("Building coreOutput...");
		List<OutputAdapter> outputAdapters = new ArrayList<>();
		// we are already given the downstream streamId
		for(Entry<Integer, Set<DataReference>> entry : output.entrySet()){
			OutputAdapter oa = OutputAdapterFactory.buildOutputAdapterForDataReference(wc, entry.getKey(), entry.getValue());
			outputAdapters.add(oa);
		}
		CoreOutput cOutput = new CoreOutput(outputAdapters);
		LOG.info("Building coreOutput...OK");
		return cOutput;
	}
}
