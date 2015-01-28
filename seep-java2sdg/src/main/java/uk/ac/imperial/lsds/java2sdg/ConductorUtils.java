package uk.ac.imperial.lsds.java2sdg;

import java.io.FileReader;
import java.io.IOException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

public class ConductorUtils {

	public Java.CompilationUnit getCompilationUnitFor(String inputFile){
		Java.CompilationUnit cu = null;
		try{
			FileReader r = new FileReader(inputFile);
			cu = new Parser(new Scanner(inputFile, r)).parseCompilationUnit();
		}
		catch(IOException io) {
			io.printStackTrace();
		} 
		catch (CompileException ce) {
			ce.printStackTrace();
		}
		return cu;
	}
	
}
