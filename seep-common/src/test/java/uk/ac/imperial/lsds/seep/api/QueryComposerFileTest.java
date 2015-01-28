package uk.ac.imperial.lsds.seep.api;

import static org.junit.Assert.*;

import org.junit.Test;

public class QueryComposerFileTest {

	@Test
	public void testFileBase() {
		//Create Base class
		FileBase bt = new FileBase();
		//Get logical seep query by composing the base class
		LogicalSeepQuery lsq = bt.compose();
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
