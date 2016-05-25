package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.analysis.LiveVariableAnalysis.LivenessInformation;
import uk.ac.imperial.lsds.java2sdg.analysis.LiveVariableAnalysis.VariableLivenessInformation;
import uk.ac.imperial.lsds.java2sdg.utils.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.utils.Util;

public class LVAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = Util.getProjectPath()+"/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		LivenessInformation map = LiveVariableAnalysis.getLVInfo(compilationUnit);
		// FIXME: adapt to new interfaces
//		for(Entry<String, VariableLivenessInformation> entry : map.entrySet()){
//			String varName = entry.getKey();
//			int livesFrom = entry.getValue().getLivesFrom();
//			int livesTo = entry.getValue().getLivesTo();
//			System.out.println("V: "+varName+" lives from: "+livesFrom+" to: "+livesTo);
//		}
	}

}
