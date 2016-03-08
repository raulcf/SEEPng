package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;

public class WorkflowAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFilePath = "/home/ra/dev/SEEPng/seep-java2sdg/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFilePath);
		
		Map<String, CodeRepr> workflowBodies = WorkflowExtractorAnalysis.getWorkflowBody(compilationUnit);
		Map<String, WorkflowRepr> map = WorkflowAnalysis.getWorkflows(inputFilePath, workflowBodies);
		for(Entry<String, WorkflowRepr> entry : map.entrySet()){
			System.out.println("name: "+entry.getKey());
			System.out.println("workflowrepr: "+entry.getValue());
		}
	}

}
