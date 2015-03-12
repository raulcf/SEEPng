package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElementRepr;

/**
 * Dumb analysis that simply create a TE per workflow. There will then be as many TEs as workflows defined in the input program.
 * In particular, this class does not do any splitting at all.
 * @author ra
 *
 */
public class CoarseGrainedTEAnalysis {

	private static int teId = 0;
	
	public static List<PartialSDGRepr> getPartialSDGs(Map<String, WorkflowRepr> workflows, Map<String, LivenessInformation> lvInfo){
		List<PartialSDGRepr> partialSDGs = new ArrayList<>();
		// Get partialSDG per workflow
		for(String workflowName : workflows.keySet()) {
			TaskElementRepr ter = new TaskElementRepr(teId++);
			// get body of workflow (code)
			// check what are the live variables in the first line -> initialVariables [extend interface of lvInfo]
			// check what are the live variables in the last line  -> outputVariables [extend interface of lvInfo]
			// check the input and outputschema
			
		}
		
		return partialSDGs;
	}
	
}
