package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.util.Traverser;

import uk.ac.imperial.lsds.java2sdg.bricks.WorkflowRepr;

public class WorkflowAnalysis extends Traverser {

	private Map<String, WorkflowRepr> workflows = new HashMap<>();
	
	public static Map<String, WorkflowRepr> getWorkflows(Java.CompilationUnit cu){
		WorkflowAnalysis wa = new WorkflowAnalysis();
		wa.traverseCompilationUnit(cu);
		return wa.workflows;
	}
	
	@Override
	public void traverseMethodInvocation(Java.MethodInvocation mi){
		String scope = mi.getEnclosingBlockStatement().getEnclosingScope().toString();
		if(scope.equals("main()")){
			System.out.println("NAME: "+mi.methodName);
		}
		
		// workflowrepr (name, source-annotation, sink-annotation, inputparameters-schema)
		// name - we get it from here
		// srcann - do we need to do a points-to analysis?
		// snkann - here the annotation annotates the method invocation
		// inputparameters - get method declaration and then read inputparameters (to avoid point-to)
	}
	
//	@Override
//	public void traverseMethodDeclarator(Java.MethodDeclarator md){
//		if(md.name.equals("main")){
//			List<? extends BlockStatement> statements = md.optionalStatements;
//			for(BlockStatement stmt : statements){
//				if(stmt instanceof Java.ExpressionStatement){
//					Java.ExpressionStatement expr = (Java.ExpressionStatement)stmt;
//					System.out.println("Expr: "+expr.toString());
//					System.out.println("RVALUE: "+expr.rvalue.toString());
//				}
////				System.out.println("SCOPE: "+stmt.getEnclosingScope().toString());
////				System.out.println("STMT: "+stmt.toString());
////				System.out.println("class: "+stmt.getClass().getSimpleName());
//			}
//			
//		}
//	}

}
