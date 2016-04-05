package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.Util;
import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;

public class AnnotationAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = Util.getProjectPath()+"/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		Map<Integer, SDGAnnotation> map = AnnotationAnalysis.getAnnotations(compilationUnit);
		for(Entry<Integer, SDGAnnotation> entry : map.entrySet()){
			System.out.println("line: "+entry.getKey()+" ann: "+entry.getValue());
		}
	}

}
