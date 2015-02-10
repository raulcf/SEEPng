package uk.ac.imperial.lsds.java2sdg.analysis;

import java.io.File;
import java.util.Map;

import org.codehaus.janino.JavaSourceClassLoader;

import uk.ac.imperial.lsds.java2sdg.api.SeepProgram;
import uk.ac.imperial.lsds.java2sdg.api.SeepProgramConfiguration;
import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;

public class WorkflowAnalysis {
	
	public static Map<String, WorkflowRepr> getWorkflows(File inputFile, String className){
		WorkflowAnalysis wa = new WorkflowAnalysis();
		SeepProgramConfiguration spc = compileAndExecuteMain(wa, inputFile, className);
		return spc.getWorkflows();
	}
	
	private static SeepProgramConfiguration compileAndExecuteMain(WorkflowAnalysis wa, File inputFile, String className){
		SeepProgramConfiguration spc = null;
		ClassLoader cl = new JavaSourceClassLoader(wa.getClass().getClassLoader());
		((JavaSourceClassLoader)cl).setSourcePath(new File[] {inputFile});
		try {
			Object o = cl.loadClass(className).newInstance();
			spc = ((SeepProgram) o).configure();
		} 
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return spc;
	}

}
