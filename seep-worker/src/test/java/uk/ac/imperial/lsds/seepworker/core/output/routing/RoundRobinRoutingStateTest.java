package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class RoundRobinRoutingStateTest {

	@Test
	public void test() {
		int connections = 100;
		List<Integer> opIds = new ArrayList<>();
		// Build a router
		for(int i = 0; i < connections; i++){
			opIds.add(i);
		}
		Router r = RouterFactory.buildRouterFor(opIds, false);
		
		// Try router
		int iter = 1000;
		for(int i = 0; i < iter; i++){
			int opId = r.route();
			System.out.println("route-to: "+opId);
		}
		assert(true);
	}
	
	@Test
	public void testTime(){
		int connections = 5;
		List<Integer> opIds = new ArrayList<>();
		// Build a router
		for(int i = 0; i < connections; i++){
			opIds.add(i);
		}
		Router r = RouterFactory.buildRouterFor(opIds, false);
		
		// Try router
		// Try router
		int iter = 10000000;
		long start = System.nanoTime();
		for(int i = 0; i < iter; i++){
			int opId = r.route();
		}
		long stop = System.nanoTime();
		long totalMillis = (stop-start)/1000000;
		System.out.println("Time to route: "+iter+" to "+connections+" downstreams is: "+totalMillis+" ms");
		assert(true);
	}

}
