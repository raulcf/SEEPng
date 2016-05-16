package algostest;

import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import api.topology.Cluster;
import api.topology.ClusterImplementation;
import ir.Traceable;
import util.DOTExporter;
import util.Utils;

public class TraceTest {
	
	@Test
	public void testTraces() {
		
		// Create program
		SUMMA s = new SUMMA();
		
		Cluster c = new ClusterImplementation();
		c.setNumberNodes(4); // cluster of 1 node
		
		// Configure program
		s.configure(c);
		
		// Run program
		s.program();
		
		// Get traces
		Map<Integer, Traceable> traces = s.api.getTraces();
		DOTExporter dex = new DOTExporter();
		dex.export(traces);
		String graphRepr = dex.getStringRepr();
		Utils.writeToDOTFile(graphRepr, "graph");
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
