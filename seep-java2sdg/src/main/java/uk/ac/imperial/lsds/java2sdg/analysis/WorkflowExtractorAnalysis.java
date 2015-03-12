package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.AlternateConstructorInvocation;
import org.codehaus.janino.Java.AssertStatement;
import org.codehaus.janino.Java.Block;
import org.codehaus.janino.Java.BlockStatement;
import org.codehaus.janino.Java.BreakStatement;
import org.codehaus.janino.Java.ContinueStatement;
import org.codehaus.janino.Java.DoStatement;
import org.codehaus.janino.Java.EmptyStatement;
import org.codehaus.janino.Java.ExpressionStatement;
import org.codehaus.janino.Java.FieldDeclaration;
import org.codehaus.janino.Java.ForEachStatement;
import org.codehaus.janino.Java.ForStatement;
import org.codehaus.janino.Java.IfStatement;
import org.codehaus.janino.Java.Initializer;
import org.codehaus.janino.Java.LabeledStatement;
import org.codehaus.janino.Java.LocalClassDeclarationStatement;
import org.codehaus.janino.Java.LocalVariableDeclarationStatement;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.Java.ReturnStatement;
import org.codehaus.janino.Java.SuperConstructorInvocation;
import org.codehaus.janino.Java.SwitchStatement;
import org.codehaus.janino.Java.SynchronizedStatement;
import org.codehaus.janino.Java.ThrowStatement;
import org.codehaus.janino.Java.TryStatement;
import org.codehaus.janino.Java.WhileStatement;
import org.codehaus.janino.Visitor.BlockStatementVisitor;
import org.codehaus.janino.util.Traverser;

public class WorkflowExtractorAnalysis extends Traverser {

	private Map<String, List<String>> bodies;
	
	public static Map<String, List<String>> getWorkflowBody(Java.CompilationUnit cu){
		WorkflowExtractorAnalysis wea = new WorkflowExtractorAnalysis();
		wea.traverseCompilationUnit(cu);
		return wea.bodies;
	}

	@Override
	public void traverseMethodDeclarator(MethodDeclarator md){
		System.out.println("METHOD: "+md.toString());
		
		BlockExplorer be = new BlockExplorer();
		List<? extends BlockStatement> methodBody = md.optionalStatements;
		for(BlockStatement bs : methodBody){
			bs.accept(be);
		}
		bodies.put(md.toString(), be.getCode());
	}
	
	class BlockExplorer implements BlockStatementVisitor{

		private List<String> code = new ArrayList<>();
		
		public List<String> getCode(){
			return code;
		}
		
		@Override
		public void visitInitializer(Initializer i) {
			String init = i.toString();
			code.add(init);
		}

		@Override
		public void visitBlock(Block b) {
			for(BlockStatement bs : b.statements){
				bs.accept(this);
			}
		}

		@Override
		public void visitExpressionStatement(ExpressionStatement es) {
			String expression = es.toString();
			code.add(expression);
		}

		@Override
		public void visitIfStatement(IfStatement is) {
			String _if = "if(";
			code.add(_if);
			String condition = is.condition.toString();
			code.add(condition);
			String if_ = ")";
			code.add(if_);
			(is.thenStatement).accept(this);
			(is.optionalElseStatement).accept(this);
		}

		@Override
		public void visitForStatement(ForStatement fs) {
			String _for = "for(";
			code.add(_for);
			(fs.optionalInit).accept(this);
			String condition = fs.optionalCondition.toString();
			code.add(condition+";");
			String update = (fs.optionalUpdate).toString();
			code.add(update+")");
			String for_ = ")";
			code.add(for_);
			(fs.body).accept(this);
		}
		
		@Override
		public void visitLocalVariableDeclarationStatement(LocalVariableDeclarationStatement lvds) {
			String lvDeclaration = lvds.toString();
			code.add(lvDeclaration);
		}

		@Override
		public void visitReturnStatement(ReturnStatement rs) {
			rs.accept(this);
		}
		
		@Override
		public void visitForEachStatement(ForEachStatement forEachStatement) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitWhileStatement(WhileStatement ws) {
			// TODO Auto-generated method stub
			
		}
		
		@Override
		public void visitTryStatement(TryStatement ts) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitSwitchStatement(SwitchStatement ss) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitSynchronizedStatement(SynchronizedStatement ss) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitDoStatement(DoStatement ds) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitThrowStatement(ThrowStatement ts) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitBreakStatement(BreakStatement bs) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitContinueStatement(ContinueStatement cs) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitAssertStatement(AssertStatement as) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitEmptyStatement(EmptyStatement es) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitLocalClassDeclarationStatement(
				LocalClassDeclarationStatement lcds) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitAlternateConstructorInvocation(
				AlternateConstructorInvocation aci) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitSuperConstructorInvocation(SuperConstructorInvocation sci) {
			// TODO Auto-generated method stub
		}
		
		@Override
		public void visitFieldDeclaration(FieldDeclaration fd) {}

		@Override
		public void visitLabeledStatement(LabeledStatement ls) {}
		
	}
	
}