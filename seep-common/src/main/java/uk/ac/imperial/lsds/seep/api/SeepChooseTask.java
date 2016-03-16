package uk.ac.imperial.lsds.seep.api;

import java.util.List;
import java.util.Map;

public interface SeepChooseTask extends SeepTask {
	
	public Integer choose(Map<Integer, List<Object>> evaluatedResults); 
	
}
