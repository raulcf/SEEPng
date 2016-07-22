package marking;
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



public class MinMaxRangeChoose implements SeepChooseTask {

	final static Logger log = LoggerFactory.getLogger(MinMaxRangeChoose.class);

	private int min = 10;
	private int max = 1000;

	public MinMaxRangeChoose() {
	}

	public MinMaxRangeChoose(int scaleFactor) {
		this.min = min * scaleFactor;
		this.max = max * scaleFactor;
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

		Set<Integer> chosenOnes = new HashSet<Integer>();
		for(Entry<Integer, List<Object>> obj : arg0.entrySet()) {
			for(Object o : obj.getValue()) {
				int count = (int)o;
				if (min <= count && count <= max) 
					chosenOnes.add(obj.getKey());
			}
		}
		return chosenOnes;
	}

}
