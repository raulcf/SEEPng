package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;

public class WorkflowExtractorAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = "/home/ra/dev/SEEPng/seep-java2sdg/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		Map<String, CodeRepr> map = WorkflowExtractorAnalysis.getWorkflowBody(compilationUnit);
		for(Entry<String, CodeRepr> entry : map.entrySet()) {
			System.out.println("method: "+entry.getKey());
			System.out.println("  "+entry.getValue());
		}
	}

}
