package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.BlockStatement;
import org.codehaus.janino.Java.ExpressionStatement;
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
	public void traverseMethodDeclarator(Java.MethodDeclarator md){
		if(md.name.equals("main")){
			List<? extends BlockStatement> statements = md.optionalStatements;
			for(BlockStatement stmt : statements){
				if(stmt instanceof Java.ExpressionStatement){
					Java.ExpressionStatement expr = (Java.ExpressionStatement)stmt;
					System.out.println("Expr: "+expr.toString());
					System.out.println("RVALUE: "+expr.rvalue.toString());
				}
//				System.out.println("SCOPE: "+stmt.getEnclosingScope().toString());
//				System.out.println("STMT: "+stmt.toString());
//				System.out.println("class: "+stmt.getClass().getSimpleName());
			}
			
		}
	}

}
