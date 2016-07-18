package masking;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepChooseTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;



public class ThresholdChoose implements SeepChooseTask {

	final static Logger log = LoggerFactory.getLogger(ThresholdChoose.class);

	private float threshold = 0.8f;

	public ThresholdChoose() {
	}

	public ThresholdChoose(float threshold) {
		this.threshold = threshold;
	}
	
	@Override
	public void close() {
	}

	@Override
	public void processData(ITuple arg0, API arg1) {
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
	}

	@Override
	public void setUp() {
	}

	@Override
	public Set<Integer> choose(Map<Integer, List<Object>> arg0) {
		log.debug("Triggered choose (threshold {}): {}", this.threshold, arg0);

		Integer last = null;
		Set<Integer> chosenOnes = new HashSet<Integer>();
		for(Entry<Integer, List<Object>> obj : arg0.entrySet()) {
			last = obj.getKey();
			for(Object o : obj.getValue()) {
				float ratio = (float)o;
				if (ratio > threshold) 
					chosenOnes.add(obj.getKey());
			}
		}
		
		if (chosenOnes.isEmpty())
			chosenOnes.add(last);
		
		return chosenOnes;
	}

}
