package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.janino.Java;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.Util;
import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;

public class StateAnalysisTest {

	@Test
	public void test() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = Util.getProjectPath()+"/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		Map<String, InternalStateRepr> map = StateAnalysis.getStates(compilationUnit);
		for(Entry<String, InternalStateRepr> entry : map.entrySet()){
			System.out.println("name: "+entry.getKey()+" id: "+entry.getValue().getStateId()
					+" ann: "+entry.getValue().getStateAnnotation()+" type: "+entry.getValue().getStateType().toString());
		}
	}

}
