package uk.ac.imperial.lsds.seepworker.core.output;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.output.routing.Router;
import uk.ac.imperial.lsds.seepworker.core.output.routing.RouterFactory;

public class OutputAdapterFactory {

	public static OutputAdapter buildOutputAdapterOfTypeNetworkForOps(WorkerConfig wc, int streamId, 
			List<DownstreamConnection> cons, PhysicalSeepQuery query){
		// Create a router for the outputAdapter with the downstreamConn info
		Router r = RouterFactory.buildRouterFor(cons);

		// Get a map of id-outputBuffer, where id is the downstream op id
		Map<Integer, OutputBuffer> outputBuffers = new HashMap<>();
		for(DownstreamConnection dc : cons){
			int id = dc.getDownstreamOperator().getOperatorId();
			PhysicalOperator downstreamPhysOperator = query.getOperatorWithId(dc.getDownstreamOperator().getOperatorId());
			Connection c = new Connection(downstreamPhysOperator.getWrappingEndPoint());
			OutputBuffer ob = new OutputBuffer(wc, id, c, streamId);
			outputBuffers.put(id, ob);
		}
		// TODO: left for configuration whether this should be a simpleoutput or something else...
		OutputAdapter oa = new SimpleNetworkOutput(streamId, r, outputBuffers);
		return oa;
	}

}
