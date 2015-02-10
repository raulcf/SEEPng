package uk.ac.imperial.lsds.seepworker.core.output.routing;

import static org.junit.Assert.*;

import org.junit.Test;

public class ConsistentHashingRoutingTest {

	@Test
	public void test() {
		int min = Integer.MIN_VALUE;
		int max = Integer.MAX_VALUE;
		
		int horizon = min + max;
		
		assert(horizon == Long.MAX_VALUE);
	}

}
