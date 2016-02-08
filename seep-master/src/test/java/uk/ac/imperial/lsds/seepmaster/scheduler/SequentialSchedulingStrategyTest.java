package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seep.scheduler.engine.ScheduleTracker;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.query.ScheduledQueryManager;

public class SequentialSchedulingStrategyTest {

	private ScheduledQueryManager boilerplate(){
		// Create schedule for one query with SequentialSchedulingStrategy
		Properties p = new Properties();
		p.setProperty("scheduling.strategy.type", "0");
		p.setProperty("properties.file", "/dev/null");
		MasterConfig mc = new MasterConfig(p);
		ComplexBranchingQuery fb = new ComplexBranchingQuery();
		SeepLogicalQuery lsq = fb.compose();
		ScheduledQueryManager se = ScheduledQueryManager.getInstance(null, null,null, mc);
		se.buildSchedulingPlanForQuery(lsq);
		
		// Set schedulerEngine ready to start
		se.__initializeEverything();
		return se;
	}
	
	@Test
	public void testStageLifecycle() {
		ScheduledQueryManager se = boilerplate();
		
		ScheduleTracker tracker = se.__tracker_for_test();
		
		Set<Stage> ready = tracker.getReadySet();
		System.out.println("ready stages: "+ready.size());
		for(Stage s : ready) {
			System.out.println("Stage READY: "+s.getStageId());
			tracker.setFinished(s, null);
		}
		
		Set<Stage> ready2 = tracker.getReadySet();
		System.out.println("ready stages: "+ready2.size());
		for(Stage s : ready2) {
			System.out.println("Stage READY: "+s.getStageId());
			tracker.setFinished(s, null);
		}
	}
	
	@Test
	public void testCorrectness(){
		ScheduledQueryManager se = boilerplate();
		
		ScheduleTracker tracker = se.__tracker_for_test();
				
		Set<Stage> ready = tracker.getReadySet();
		System.out.println("ready stages: "+ready.size());
		for(Stage s : ready) {
			System.out.println("Stage READY: "+s.getStageId());
		}
		
		boolean finished = false;
		while(!finished) {
			Stage next = se.__get_next_stage_to_schedule_fot_test();
			tracker.setFinished(next, null);
			
			if(next.getStageType().equals(StageType.SINK_STAGE)) {
				finished = true;
			}
			System.out.println("schedule -> "+next.getStageId());
		}
	}
	
	@Test
	public void testLocalSchedulingSpeed() {
		ScheduledQueryManager se = boilerplate();
		
		// Attempt to make the following number of schedules by reseting the query as necessary
		ScheduleTracker tracker = se.__tracker_for_test();
		int numSchedules = 100000;
		long start = System.currentTimeMillis();
		while(numSchedules > 0) {
			Stage next = se.__get_next_stage_to_schedule_fot_test();
			tracker.setFinished(next, null);
			if(next.getStageType().equals(StageType.SINK_STAGE)) {
				se.__reset_schedule();
				se.__initializeEverything();
			}
			numSchedules--;
		}
		long stop = System.currentTimeMillis();
		System.out.println("total time to schedule: "+numSchedules+" -> "+(stop-start)+" ms");
	}
	
	@Test
	public void testLocalSchedulingSpeedWithLargerQuery() {
		// Create schedule for one query with SequentialSchedulingStrategy
		Properties p = new Properties();
		p.setProperty("scheduling.strategy.type", "0");
		p.setProperty("properties.file", "/dev/null");
		MasterConfig mc = new MasterConfig(p);
		ComplexManyBranchesMultipleSourcesQuery fb = new ComplexManyBranchesMultipleSourcesQuery();
		SeepLogicalQuery lsq = fb.compose();
		ScheduledQueryManager se = ScheduledQueryManager.getInstance(null, null,null, mc);
		se.buildSchedulingPlanForQuery(lsq);
		
		// Set schedulerEngine ready to start
		se.__initializeEverything();
		
		// Attempt to make the following number of schedules by reseting the query as necessary
		ScheduleTracker tracker = se.__tracker_for_test();
		int numSchedules = 100000;
		long start = System.currentTimeMillis();
		while(numSchedules > 0) {
			Stage next = se.__get_next_stage_to_schedule_fot_test();
			tracker.setFinished(next, null);
			if(next.getStageType().equals(StageType.SINK_STAGE)) {
				se.__reset_schedule();
				se.__initializeEverything();
			}
			numSchedules--;
		}
		long stop = System.currentTimeMillis();
		System.out.println("total time to schedule (larger): "+numSchedules+" -> "+(stop-start)+" ms");
		/**
		 * 0.5 sec for 100K schedules in a crappy machine. Let's assume linear time growth ->
		 * 1 sec for 200K. 1sec/200K = 0.000005. Tasks plus network latency should be below 50us, hmmmmm we're fine.
		 */
	}

}
