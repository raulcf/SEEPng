package uk.ac.imperial.lsds.java2sdg;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

public class ConductorUtils {

	/**
	 * Returns a Compilation Unit (a Janino Class Wrapper abstraction for a given java file)
	 * Note that the Compilation unit is the higher level of abstraction we need for java2sdg
	 * @param inputFile
	 * @return Java.CompilationUnit
	 */
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
	
	/**
	 * Returns a file instance of the path given as parameter
	 * @param inputFilePath
	 * @return java.io.File
	 */
	public File getFile(String inputFilePath) {
		return new File(inputFilePath);
	}

	/**
	 * Returns the classs name of a Java Class file
	 * For example: the className of the file /home/pgaref/test.java is test
	 * @param inputFile
	 * @return String (class Name)
	 */
	public String getClassName(File inputFile) {
		String inputFileName = inputFile.getName();
		String className = inputFileName.substring(0, inputFileName.indexOf("."));
		return className;
	}
	
}
