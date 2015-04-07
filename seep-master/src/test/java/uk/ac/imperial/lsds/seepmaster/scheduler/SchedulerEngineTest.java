package uk.ac.imperial.lsds.seepmaster.scheduler;

import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class SchedulerEngineTest {

//	@Test
//	public void test() {
//		SimplePipelineQuery fb = new SimplePipelineQuery();
//		SeepLogicalQuery lsq = fb.compose();
//		SchedulerEngine se = SchedulerEngine.getInstance(null);
//		ScheduleDescription sd = se.buildSchedulingPlanForQuery(lsq);
//		Set<Stage> stages = sd.getStages();
//		for(Stage s : stages){
//			System.out.println("STAGE: ");
//			System.out.println("");
//			System.out.println("");
//			System.out.println(s);
//		}
//		assertTrue(true);
//	}
	
//	@Test
//	public void testBranching(){
//		ComplexBranchingQuery fb = new ComplexBranchingQuery();
//		SeepLogicalQuery lsq = fb.compose();
//		SchedulerEngine se = SchedulerEngine.getInstance(null);
//		ScheduleDescription sd = se.buildSchedulingPlanForQuery(lsq);
//		Set<Stage> stages = sd.getStages();
//		System.out.println("STAGES: "+stages.size());
//		for(Stage s : stages){
//			System.out.println("stage: "+s.getStageId());
//		}
//		for(Stage s : stages){
//			System.out.println("STAGE: ");
//			System.out.println("");
//			System.out.println("");
//			System.out.println(s);
//		}
//		assertTrue(true);
//	}
	
	
	@Test
	public void testBranching(){
		ComplexManyBranchesMultipleSourcesQuery fb = new ComplexManyBranchesMultipleSourcesQuery();
		SeepLogicalQuery lsq = fb.compose();
		SchedulerEngine se = SchedulerEngine.getInstance(null);
		ScheduleDescription sd = se.buildSchedulingPlanForQuery(lsq);
		Set<Stage> stages = sd.getStages();
		System.out.println("STAGES: "+stages.size());
		for(Stage s : stages){
			System.out.println("stage: "+s.getStageId());
		}
		for(Stage s : stages){
			System.out.println("STAGE: ");
			System.out.println("");
			System.out.println("");
			System.out.println(s);
		}
		assertTrue(true);
	}

}
