package uk.ac.imperial.lsds.seepworker.core.output.routing;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepQueryPhysicalOperator;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;

public class RoundRobinRoutingStateTest {

	@Test
	public void test() {
		int iter = 10;
		Properties p = new Properties();
		p.setProperty("master.ip", "127.0.0.1");
		p.setProperty("batch.size", "10000"); // 25 - 25 - 400
		p.setProperty("rx.buffer.size", "100000"); // 66 - 116 - 817
		p.setProperty("tx.buffer.size", "100000"); // 66 - 116 - 817
		p.setProperty("properties.file", "");
		WorkerConfig fake = new WorkerConfig(p);
		List<DownstreamConnection> cons = new ArrayList<>();
		Map<Integer, OutputBuffer> obufs = new HashMap<>();
		// fill cons
		for(int i = 0; i < 3; i++){
			DownstreamConnection con = new DownstreamConnection(null, i, null, null, null);
			cons.add(con);
			obufs.put(i, new OutputBuffer(fake, i, null, 0));
		}
		Router r = RouterFactory.buildRouterFor(cons, false);
		
		for(int i = 0; i < iter; i++){
			OutputBuffer ob = r.route(obufs);
			System.out.println("CHOSEN: "+ob.id());
		}
		assert(true);
	}

}
