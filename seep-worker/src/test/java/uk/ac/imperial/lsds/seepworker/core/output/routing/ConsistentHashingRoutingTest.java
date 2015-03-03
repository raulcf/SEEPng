package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

public class ConsistentHashingRoutingTest {
	
	@Test
	public void test() {
		int connections = 5000;
		List<Integer> opIds = new ArrayList<>();
		// Build a router
		for(int i = 0; i < connections; i++){
			opIds.add(i);
		}
		Router r = RouterFactory.buildRouterFor(opIds, true);
		
		// Try router
		Map<Integer, Integer> distribution = new HashMap<>();
		int iter = 100000;
		for(int key = 0; key < iter; key++){
			int opId = r.route(key);
			routeTo(distribution, opId, key);
		}
		printDistribution(distribution, iter);
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
		Router r = RouterFactory.buildRouterFor(opIds, true);
		
		// Try router
		int iter = 10000000;
		long start = System.nanoTime();
		for(int key = 0; key < iter; key++){
			int opId = r.route(key);
		}
		long stop = System.nanoTime();
		long totalMillis = (stop-start)/1000000;
		System.out.println("Time to route: "+iter+" to "+connections+" downstreams is: "+totalMillis+" ms");
		assert(true);
	}
	
	private void routeTo(Map<Integer, Integer> distribution, int opId, int key){
		int newValue = 1;
		if(distribution.containsKey(opId)){
			newValue = distribution.get(opId) + 1;
		}
		distribution.put(opId, newValue);
	}
	
	private void printDistribution(Map<Integer, Integer> distribution, int iter){
		int keySet = distribution.size();
		int avgDistribution = iter/keySet;
		System.out.println("Expected average distro per key: "+avgDistribution);
		for(Entry<Integer, Integer> entry : distribution.entrySet()){
			System.out.println("id: "+entry.getKey()+" #:"+entry.getValue());
		}
		
	}

}
