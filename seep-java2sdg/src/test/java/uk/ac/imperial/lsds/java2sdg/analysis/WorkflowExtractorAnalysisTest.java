package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.List;
import java.util.Map;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;

public class WorkflowExtractorAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = "/home/ra/dev/SEEPng/seep-java2sdg/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		Map<String, List<String>> map = WorkflowExtractorAnalysis.getWorkflowBody(compilationUnit);
//		for(Entry<Integer, SDGAnnotation> entry : map.entrySet()){
//			System.out.println("line: "+entry.getKey()+" ann: "+entry.getValue());
//		}
	}

}
