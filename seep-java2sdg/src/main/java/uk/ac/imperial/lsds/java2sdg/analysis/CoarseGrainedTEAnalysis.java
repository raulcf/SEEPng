package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGComponents;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElementRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.VariableRepr;
import uk.ac.imperial.lsds.seep.api.DataStore;

/**
 * Dumb analysis that simply create a TE per workflow. There will then be as many TEs as workflows defined in the 
 * input program.
 * @author ra
 *
 */
public class CoarseGrainedTEAnalysis {

	private static int teId = 0;
	
	public static List<PartialSDGRepr> getPartialSDGs(Map<String, WorkflowRepr> workflows, LivenessInformation lvInfo){
		List<PartialSDGRepr> partialSDGs = new ArrayList<>();
		// Get partialSDG per workflow
		
		for(Entry<String, WorkflowRepr> entry : workflows.entrySet()){
			List<PartialSDGComponents> psdgComponents = new ArrayList<>();
			
			String workflowName = entry.getKey();
			WorkflowRepr workflow = entry.getValue();
			
			// get body of workflow (code)
			CodeRepr code = workflow.getCode();
			
			TaskElementRepr ter = new TaskElementRepr(teId++);
			ter.setCode(code.getCodeText());
			ter.setOutputSchema(entry.getValue().getOutputSchema());
			
			// check what are the live variables in the first line
			List<VariableRepr> inputVariables = lvInfo.getLiveVarsAt(code.getInitLine());
			// check what are the live variables in the last line
			List<VariableRepr> outputVariables = lvInfo.getLiveVarsAt(code.getEndLine());
			
			// check the input and outputschema
			DataStore source = workflow.getSource();
			DataStore sink = workflow.getSink();
			
			// Group all variables into list of partialSDGComponents (in this case, there is only one TE)
			PartialSDGComponents pComponents = new PartialSDGComponents(ter, inputVariables, outputVariables);
			psdgComponents.add(pComponents);
			
			PartialSDGRepr psdg = PartialSDGRepr.makePartialSDGRepr(workflowName, source, psdgComponents, sink);
			partialSDGs.add(psdg);
		}
		
		return partialSDGs;
	}
	
}
