package uk.ac.imperial.lsds.java2sdg.analysis.strategies;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java.BasicType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

import uk.ac.imperial.lsds.java2sdg.analysis.LiveVariableAnalysis;
import uk.ac.imperial.lsds.java2sdg.analysis.LiveVariableAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGComponent;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.VariableRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.TaskElement;
import uk.ac.imperial.lsds.seep.api.DataStore;

/**
 * Dumb analysis that simply create a TE per workflow. There will then be as many TEs as workflows defined in the 
 * input program.
 * @author ra
 *
 */
public class CoarseGrainedTEAnalysis {

	private static int teId = 0;
	private final static Logger LOG = LoggerFactory.getLogger(CoarseGrainedTEAnalysis.class.getSimpleName());
	
	public static List<PartialSDGRepr> getPartialSDGs(Map<String, WorkflowRepr> workflows, LivenessInformation lvInfo){
		List<PartialSDGRepr> partialSDGs = new ArrayList<>();
		// Get partialSDG per workflow
		
		for(Entry<String, WorkflowRepr> entry : workflows.entrySet()){
			
			List<PartialSDGComponent> psdgComponents = new ArrayList<>();
			String workflowName = entry.getKey();
			WorkflowRepr workflow = entry.getValue();	
			LOG.info("Creating Partial SDG for Workflow: {} ", workflowName);
			
			/**
			 * Extra workflow attributes needed for TEs
			 */
			CodeRepr code = workflow.getCode();
			DataStore source = workflow.getSource();
			DataStore sink = workflow.getSink();
			
			TaskElement ter = new TaskElement(teId++);
			ter.setCode(code.getCodeText());
			
			//Case Workflow does have outputDataStore
			if( sink != null )
				ter.setOutputSchema(sink.getSchema());
			if( source == null )
				LOG.error("Workflow {} has NO inputDataStore", workflowName);
			
			// check what are the live variables in the first line
			List<VariableRepr> inputVariables = lvInfo.getLiveInputVarsAt(workflowName, code.getInitLine());
			// check what are the live variables in the last line
			List<VariableRepr> outputVariables = lvInfo.getLiveOutputVarsAt(workflowName, code.getEndLine());
			
			//What about Schema vars?? add them here!
			//TODO: pgaref FIX STRING - JAnino type - hardcoded to bytes!
			for( String var : source.getSchema().names() ) {
				if(source.getSchema().getField(var).toString() == "INT"){
					inputVariables.add(VariableRepr.var(new BasicType(null, BasicType.INT), var));
					outputVariables.add(VariableRepr.var(new BasicType(null, BasicType.INT), var));
				}
				else if(source.getSchema().getField(var).toString() == "STRING"){
					inputVariables.add(VariableRepr.var(new BasicType(null, BasicType.BYTE), var));
					outputVariables.add(VariableRepr.var(new BasicType(null, BasicType.BYTE), var));
				}
				else if(source.getSchema().getField(var).toString() == "LONG"){
					inputVariables.add(VariableRepr.var(new BasicType(null, BasicType.LONG), var));
					outputVariables.add(VariableRepr.var(new BasicType(null, BasicType.LONG), var));
				}
			}
			
			
			
//			System.out.println("--------IN------------");
//			inputVariables.forEach( var -> System.out.println(var.getName()));
//			System.out.println("--------OUT-----------");
//			outputVariables.forEach( var -> System.out.println(var.getName()));
//			System.out.println("---------------------");
			
			
			// Group all variables into list of partialSDGComponents (in this case, there is only one TE)
			PartialSDGComponent pComponent = new PartialSDGComponent(ter, inputVariables, outputVariables);
			psdgComponents.add(pComponent);
			
			PartialSDGRepr psdg = PartialSDGRepr.makePartialSDGRepr(workflowName, source, psdgComponents, sink);
			partialSDGs.add(psdg);
		}
		
		return partialSDGs;
	}
	
}
