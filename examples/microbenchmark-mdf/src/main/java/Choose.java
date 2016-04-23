import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepChooseTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;



public class Choose implements SeepChooseTask {

	private boolean incremental;
	
	public Choose() { }
	
	public Choose(boolean incremental) {
		this.incremental = incremental;
	}
	
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
	public List<Integer> choose(Map<Integer, List<Object>> arg0) {
		List<Integer> chosenOne = new ArrayList<>();
		// Non incremental execution
		if(!incremental) {
			for(Entry<Integer, List<Object>> obj : arg0.entrySet()) {
				chosenOne.add(obj.getKey());
			}
			return chosenOne;
		}
		
		System.out.println("CHOOSE btw:");
		for(Entry<Integer, List<Object>> obj : arg0.entrySet()) {
			System.out.print(obj.getKey() + " or ");
		}
		long highestQuality = -666;
		for(Entry<Integer, List<Object>> obj : arg0.entrySet()) {
			for(Object o : obj.getValue()) {
				// I know the type, as I wrote the evaluators
				long quality = (long)o;
				System.out.println("is " +quality+ " > " +highestQuality + " ?");
				if(quality > highestQuality) {
					highestQuality = quality;
					chosenOne.add(obj.getKey());
				}
			}
		}
		System.out.println("CHOSEN one: " + chosenOne);
		return chosenOne;
	}

}
