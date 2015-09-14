package uk.ac.imperial.lsds.seepworker.core;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.testutils.LongPipelineBase;

public class ScheduleTaskTest {

	@Test
	public void testCreateScheduleTasks() {
		LongPipelineBase lpb = new LongPipelineBase();
		SeepLogicalQuery lsq = lpb.compose();
		
		// Create stage manually (scheduler not accessible from this module)
		
		Stage s1 = new Stage(0);
		s1.add(-1);
		Stage s2 = new Stage(1);
		s2.add(-10);
		Stage s3 = new Stage(2);
		s3.add(1);
		s3.add(2);
		s3.add(3);
		Stage s4 = new Stage(3);
		s4.add(4);
		Stage s5 = new Stage(4);
		s5.add(5);
		Stage s6 = new Stage(5);
		s6.add(-2);
		
		// Build scheduleTasks from Stages
		ScheduleTask st1 = ScheduleTask.buildTaskFor(0, s1, lsq);
		ScheduleTask st2 = ScheduleTask.buildTaskFor(0, s2, lsq);
		ScheduleTask st3 = ScheduleTask.buildTaskFor(0, s3, lsq);
		ScheduleTask st4 = ScheduleTask.buildTaskFor(0, s4, lsq);
		ScheduleTask st5 = ScheduleTask.buildTaskFor(0, s5, lsq);
		ScheduleTask st6 = ScheduleTask.buildTaskFor(0, s6, lsq);

		// setUp tasks
		
		// print tasks for visual inspection
		
		// run tasks
		
	}

}
