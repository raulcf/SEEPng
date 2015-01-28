package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.Annotation;
import org.codehaus.janino.util.Traverser;

import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;

public class AnnotationAnalysis extends Traverser {

	private AnalysisUtils au = new AnalysisUtils();
	private Map<Integer, SDGAnnotation> anns = new HashMap<>();
	
	public static Map<Integer, SDGAnnotation> getAnnotations(Java.CompilationUnit cu){
		AnnotationAnalysis aa = new AnnotationAnalysis();
		aa.traverseCompilationUnit(cu);
		return aa.anns;
	}
	
	@Override 
	public void traverseFieldDeclaration(Java.FieldDeclaration fd) {
		Annotation[] annotations = fd.getAnnotations();
		for(Annotation ann : annotations){
			SDGAnnotation a = au.identifyAnnotation(ann);
			int line = fd.getLocation().getLineNumber();
			anns.put(line, a);
		}
	}
	
	@Override 
    public void traverseLocalVariableDeclarationStatement(Java.LocalVariableDeclarationStatement lvds) {
		Java.Modifiers mods = lvds.modifiers;
        for(Annotation ann : mods.annotations){
        	SDGAnnotation a = au.identifyAnnotation(ann);
        	int line = lvds.getLocation().getLineNumber();
			anns.put(line, a);
        }
	}
	
	// TODO: write a visitor for methods so that we can retrieve Collection annotation
	
}
