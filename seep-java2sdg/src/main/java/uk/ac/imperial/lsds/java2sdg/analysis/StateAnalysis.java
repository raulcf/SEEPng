package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.Annotation;
import org.codehaus.janino.Java.Type;
import org.codehaus.janino.Java.VariableDeclarator;
import org.codehaus.janino.util.Traverser;

import uk.ac.imperial.lsds.java2sdg.LimitationException;
import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;

public class StateAnalysis extends Traverser {

	private AnalysisUtils au = new AnalysisUtils();
	private Map<String, InternalStateRepr> states = new HashMap<>();
	private int stateId;
	
	public static Map<String, InternalStateRepr> getStates(Java.CompilationUnit cu){
		StateAnalysis sa = new StateAnalysis();
		sa.traverseCompilationUnit(cu);
		return sa.states;
	}
	
	@Override 
	public void traverseFieldDeclaration(Java.FieldDeclaration fd) {
		Type t = fd.type;
		String name = null;
		VariableDeclarator[] decls = fd.variableDeclarators;
		if(decls.length > 1){
			throw new LimitationException("Only one field/variable declared per line is supported");
		}
		for(VariableDeclarator vd : decls){
			name = vd.name;
		}
		SDGAnnotation annotation = null;
		Annotation[] annotations = fd.getAnnotations();
		for(Annotation ann : annotations){
			annotation = au.identifyAnnotation(ann);
		}
		InternalStateRepr isr = new InternalStateRepr(stateId, t, name, annotation);
		stateId++;
		states.put(name, isr);
	}
	
}