package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.LivenessInformation;

public class LVAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = "/home/ra/dev/SEEPng/seep-java2sdg/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		Map<String, LivenessInformation> map = LVAnalysis.getLVInfo(compilationUnit);
		for(Entry<String, LivenessInformation> entry : map.entrySet()){
			String varName = entry.getKey();
			int livesFrom = entry.getValue().getLivesFrom();
			int livesTo = entry.getValue().getLivesTo();
			System.out.println("V: "+varName+" lives from: "+livesFrom+" to: "+livesTo);
		}
	}

}
