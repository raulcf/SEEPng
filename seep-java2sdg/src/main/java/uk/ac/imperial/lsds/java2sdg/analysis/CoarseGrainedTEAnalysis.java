package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.TaskElementRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.SDG.VariableRepr;

/**
 * Dumb analysis that simply create a TE per workflow. There will then be as many TEs as workflows defined in the input program.
 * In particular, this class does not do any splitting at all.
 * @author ra
 *
 */
public class CoarseGrainedTEAnalysis {

	private static int teId = 0;
	
	public static List<PartialSDGRepr> getPartialSDGs(Map<String, WorkflowRepr> workflows, LivenessInformation lvInfo){
		List<PartialSDGRepr> partialSDGs = new ArrayList<>();
		// Get partialSDG per workflow
		
		for(Entry<String, WorkflowRepr> entry : workflows.entrySet()){
			String workflowName = entry.getKey();
			// get body of workflow (code)
			CodeRepr code = entry.getValue().getCode();
			TaskElementRepr ter = new TaskElementRepr(teId++);
			// check what are the live variables in the first line
			List<VariableRepr> inputVariables = lvInfo.getLiveVarsAt(code.getInitLine());
			// check what are the live variables in the last line
			List<VariableRepr> outputVariables = lvInfo.getLiveVarsAt(code.getEndLine());
			// check the input and outputschema
			
		}
		
		return partialSDGs;
	}
	
}
