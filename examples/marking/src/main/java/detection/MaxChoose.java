package detection;
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



public class MaxChoose implements SeepChooseTask {

	final static Logger log = LoggerFactory.getLogger(MaxChoose.class);

	public MaxChoose() {
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

		Integer chosen = null;
		int currentMax = -1;
		for(Entry<Integer, List<Object>> obj : arg0.entrySet()) {
			for(Object o : obj.getValue()) {
				int count = (int)o;
				
				if (count > currentMax) {
					currentMax = count;
					chosen = obj.getKey();
				}
			}
		}
		
		Set<Integer> chosenOnes = new HashSet<Integer>();
		chosenOnes.add(chosen);
		return chosenOnes;
	}

}
