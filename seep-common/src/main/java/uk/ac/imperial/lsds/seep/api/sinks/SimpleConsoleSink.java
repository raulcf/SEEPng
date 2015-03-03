package uk.ac.imperial.lsds.seep.api.sinks;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;

public class SimpleConsoleSink implements SeepTask, Sink {

	private int numTuple = 0;
	
	@Override
	public void setUp() {
		// TODO: Check that there's a console available and that we have access to it
		// FIXME: assume that is the case
	}

	@Override
	public void processData(ITuple data, API api) {
		// Simply print every tuple with an always increasing number
		System.out.println(numTuple+" - "+data.toString());
		numTuple++;
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
