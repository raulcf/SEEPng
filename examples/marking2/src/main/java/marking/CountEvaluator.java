package marking;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class CountEvaluator implements SeepTask {

	final static Logger log = LoggerFactory.getLogger(CountEvaluator.class);

	private int tupleCount =  0;
	
	public CountEvaluator() {
	}
	
	@Override
	public void close() {
	}

	@Override
	public void processData(ITuple data, API api) {
		tupleCount++;
		
		// store the current count
		api.storeEvaluateResults(tupleCount); 
		
		//log.debug("Current count: {}", tupleCount);
		
		// propagate the results downstream
		api.send(data.getData());
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		for (ITuple t : arg0)
			this.processData(t, arg1);
	}

	@Override
	public void setUp() {
	}

}
