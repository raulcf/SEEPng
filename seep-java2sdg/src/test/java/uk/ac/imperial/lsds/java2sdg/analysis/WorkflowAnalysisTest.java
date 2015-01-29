package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;

public class WorkflowAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = "/home/ra/dev/SEEPng/seep-java2sdg/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		Map<String, WorkflowRepr> map = WorkflowAnalysis.getWorkflows(compilationUnit);
//		for(Entry<String, WorkflowRepr> entry : map.entrySet()){
//			
//		}
	}

}
