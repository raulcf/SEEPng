package uk.ac.imperial.lsds.seepworker.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;

public class ScheduleTaskTest {

	@Test
	public void test() {
		API api = createAPI();
		ScheduleTask task = createForTest();
		
		task.setUp();
		
		List<ITuple> iTuples = new ArrayList();
		
		for(ITuple data : iTuples) {
			task.processData(data, api);
		}
		
		
	}
	
	private API createAPI(){
		
		return null;
	}
	
	private ScheduleTask createForTest(){
		
		return null;
	}

}
