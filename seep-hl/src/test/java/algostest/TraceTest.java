package algostest;

import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import api.topology.Cluster;
import api.topology.ClusterImplementation;
import ir.Traceable;

public class TraceTest {
	
	@Test
	public void testTraces() {
		
		// Create program
		SUMMA s = new SUMMA();
		
		Cluster c = new ClusterImplementation();
		c.setNumberNodes(1); // cluster of 1 node
		
		// Configure program
		s.configure(c);
		
		// Run program
		s.program();
		
		// Get traces
		Map<Integer, Traceable> traces = s.api.getTraces();
		for(Entry<Integer, Traceable> t : traces.entrySet()) {
			System.out.println("###########");
			System.out.println("###########");
			System.out.println("KEY -> " + t.getKey());
			System.out.println();
			System.out.println(t.getValue());
			System.out.println();
		}
	}
}
