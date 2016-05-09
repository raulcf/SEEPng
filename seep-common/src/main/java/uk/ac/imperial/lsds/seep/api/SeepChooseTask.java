package uk.ac.imperial.lsds.seep.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SeepChooseTask extends SeepTask {
	
	public Set<Integer> choose(Map<Integer, List<Object>> evaluatedResults); 
	
}
