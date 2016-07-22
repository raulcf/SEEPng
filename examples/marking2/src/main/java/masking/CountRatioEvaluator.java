package masking;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class CountRatioEvaluator implements SeepTask {

	final static Logger log = LoggerFactory.getLogger(CountRatioEvaluator.class);

	private int tupleCountTotal = -1;
	private int tupleCountClean =  0;

	public CountRatioEvaluator() {
		
	}

	public CountRatioEvaluator(int tupleCountTotal) {
		this.tupleCountTotal = tupleCountTotal;
		log.debug("Total count: {}", tupleCountTotal); 
	}
	
	@Override
	public void close() {
	}

    int i = 0;
    int total = 1000;

	@Override
	public void processData(ITuple data, API api) {
		tupleCountClean++;

        i++;

        if(i > total) {
            i = 0;
		    log.debug("Current ratio: {}", (1f*tupleCountClean)/tupleCountTotal);
        }

		// store the current filtering ratio
		api.storeEvaluateResults((1f*tupleCountClean)/tupleCountTotal); 
		
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
