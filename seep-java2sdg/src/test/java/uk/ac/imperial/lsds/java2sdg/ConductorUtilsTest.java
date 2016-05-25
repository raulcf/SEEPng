package uk.ac.imperial.lsds.java2sdg;

import java.util.List;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.CompilationUnit.ImportDeclaration;
import org.junit.Test;

import uk.ac.imperial.lsds.java2sdg.utils.ConductorUtils;
import uk.ac.imperial.lsds.java2sdg.utils.Util;

public class ConductorUtilsTest {

	@Test
	public void testGetCompilationUnit() {
		ConductorUtils cu = new ConductorUtils();
		String inputFile = Util.getProjectPath()+"/src/test/java/Fake.java";
		Java.CompilationUnit compilationUnit = cu.getCompilationUnitFor(inputFile);
		
		System.out.println("Compilation-Unit: ");
		System.out.println(compilationUnit.toString());
		// PRINT IMPORTS
		List<ImportDeclaration> imports = compilationUnit.importDeclarations;
		System.out.println("IMPORTS: ");
		for(ImportDeclaration _import : imports){
			System.out.println(_import.toString());
		}
	}
	
}