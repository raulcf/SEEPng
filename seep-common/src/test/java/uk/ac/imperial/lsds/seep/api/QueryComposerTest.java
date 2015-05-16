package uk.ac.imperial.lsds.seep.api;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public class QueryComposerTest {

	@Test
	public void testBase() {
		//Create Base class
		BaseTest bt = new BaseTest();
		//Get logical seep query by composing the base class
		SeepLogicalQuery lsq = bt.compose();
		System.out.println(lsq.toString());
		for(Operator lo : lsq.getAllOperators()){
			System.out.println(lo.toString());
			System.out.println("     ");
			System.out.println("     ");
			System.out.println("     ");
		}
		assertTrue(true);
	}

}
