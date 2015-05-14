package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepcontrib.kafka.comm.KafkaOutputAdapter;
import uk.ac.imperial.lsds.seepcontrib.kafka.config.KafkaConfig;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;
import uk.ac.imperial.lsds.seepworker.core.output.routing.RouterFactory;

public class OutputAdapterFactory {

	public static OutputAdapter buildOutputAdapterOfTypeNetworkForOps(WorkerConfig wc, int streamId, 
			List<DownstreamConnection> cons, Map<Integer, EndPoint> mapping){
		// Create a router for the outputAdapter with the downstreamConn info
		Router r = RouterFactory.buildRouterFor(cons);

		// Get a map of id-outputBuffer, where id is the downstream op id
		Map<Integer, OutputBuffer> outputBuffers = new HashMap<>();
		for(DownstreamConnection dc : cons){
			int opId = dc.getDownstreamOperator().getOperatorId();
			Connection c = new Connection(mapping.get(opId));
			// Get properties required by OutputBuffer
			int batch_size = wc.getInt(WorkerConfig.BATCH_SIZE);
			OutputBuffer ob = new OutputBuffer(opId, c, streamId, batch_size);
			outputBuffers.put(opId, ob);
		}
		// TODO: left for configuration whether this should be a simpleoutput or something else...
		OutputAdapter oa = new SimpleNetworkOutput(streamId, r, outputBuffers);
		return oa;
	}
	
	public static OutputAdapter buildOutputAdapterOfTypeKafkaForOps(KafkaConfig kc, int streamId, List<DownstreamConnection> cons){
		OutputAdapter oa = new KafkaOutputAdapter(kc.getString(KafkaConfig.KAFKA_SERVER), kc.getString(KafkaConfig.BASE_TOPIC),
				kc.getString(KafkaConfig.PRODUCER_CLIENT_ID), streamId);
		return oa;
	}

}
