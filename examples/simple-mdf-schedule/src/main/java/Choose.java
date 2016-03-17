import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepChooseTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class Choose implements SeepChooseTask {

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processData(ITuple arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Integer choose(Map<Integer, List<Object>> arg0) {
		long highestQuality = -666;
		int chosenOne = -666;
		for(Entry<Integer, List<Object>> obj : arg0.entrySet()) {
			for(Object o : obj.getValue()) {
				// I know the type, as I wrote the evaluators
				long quality = (long)o;
				if(quality > highestQuality) {
					highestQuality = quality;
					chosenOne = obj.getKey();
				}
			}
		}
		return chosenOne;
	}

}
