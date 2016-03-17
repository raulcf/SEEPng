package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.Util;
import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.analysis.LVAnalysis.VariableLivenessInformation;

public class LVAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = Util.getProjectPath()+"/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		LivenessInformation map = LVAnalysis.getLVInfo(compilationUnit);
		// FIXME: adapt to new interfaces
//		for(Entry<String, VariableLivenessInformation> entry : map.entrySet()){
//			String varName = entry.getKey();
//			int livesFrom = entry.getValue().getLivesFrom();
//			int livesTo = entry.getValue().getLivesTo();
//			System.out.println("V: "+varName+" lives from: "+livesFrom+" to: "+livesTo);
//		}
	}

}
