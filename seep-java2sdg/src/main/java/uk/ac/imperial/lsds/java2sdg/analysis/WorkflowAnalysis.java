package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.Map;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.janino.Java;
import org.codehaus.janino.Java.ConstructorDeclarator;
import org.codehaus.janino.Java.FieldDeclaration;
import org.codehaus.janino.Java.Initializer;
import org.codehaus.janino.Java.MemberClassDeclaration;
import org.codehaus.janino.Java.MemberInterfaceDeclaration;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.Java.Scope;
import org.codehaus.janino.Java.TypeBodyDeclaration;
import org.codehaus.janino.Java.TypeDeclaration;
import org.codehaus.janino.Visitor.TypeBodyDeclarationVisitor;
import org.codehaus.janino.util.Traverser;

import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;

public class WorkflowAnalysis extends Traverser {

	private Map<String, WorkflowRepr> workflows;
	
	public static Map<String, WorkflowRepr> getWorkflows(Java.CompilationUnit cu){
		WorkflowAnalysis wa = new WorkflowAnalysis();
		wa.traverseCompilationUnit(cu);
		return wa.workflows;
	}
	
	@Override
	public void traverseMethodDeclarator(Java.MethodDeclarator md){
		if(md.name.equals("main")){
			
			
		}
	}
	
}
