package uk.ac.imperial.lsds.seepmaster.scheduler;

import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.MasterConfig;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.query.ScheduledQueryManager;

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
		Properties p = new Properties();
		p.setProperty(MasterConfig.SCHED_STRATEGY, "");
		MasterConfig mc = new MasterConfig(p);
		ScheduledQueryManager se = ScheduledQueryManager.getInstance(null, null,null, mc);
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
