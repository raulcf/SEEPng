package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.AlternateConstructorInvocation;
import org.codehaus.janino.Java.AmbiguousName;
import org.codehaus.janino.Java.ArrayAccessExpression;
import org.codehaus.janino.Java.ArrayLength;
import org.codehaus.janino.Java.ArrayType;
import org.codehaus.janino.Java.AssertStatement;
import org.codehaus.janino.Java.Assignment;
import org.codehaus.janino.Java.BasicType;
import org.codehaus.janino.Java.BinaryOperation;
import org.codehaus.janino.Java.Block;
import org.codehaus.janino.Java.BlockStatement;
import org.codehaus.janino.Java.BooleanLiteral;
import org.codehaus.janino.Java.BreakStatement;
import org.codehaus.janino.Java.Cast;
import org.codehaus.janino.Java.CharacterLiteral;
import org.codehaus.janino.Java.ClassLiteral;
import org.codehaus.janino.Java.ConditionalExpression;
import org.codehaus.janino.Java.ContinueStatement;
import org.codehaus.janino.Java.Crement;
import org.codehaus.janino.Java.DoStatement;
import org.codehaus.janino.Java.EmptyStatement;
import org.codehaus.janino.Java.ExpressionStatement;
import org.codehaus.janino.Java.FieldAccess;
import org.codehaus.janino.Java.FieldAccessExpression;
import org.codehaus.janino.Java.FieldDeclaration;
import org.codehaus.janino.Java.FloatingPointLiteral;
import org.codehaus.janino.Java.ForEachStatement;
import org.codehaus.janino.Java.ForStatement;
import org.codehaus.janino.Java.IfStatement;
import org.codehaus.janino.Java.Initializer;
import org.codehaus.janino.Java.Instanceof;
import org.codehaus.janino.Java.IntegerLiteral;
import org.codehaus.janino.Java.LabeledStatement;
import org.codehaus.janino.Java.LocalClassDeclarationStatement;
import org.codehaus.janino.Java.LocalVariableAccess;
import org.codehaus.janino.Java.LocalVariableDeclarationStatement;
import org.codehaus.janino.Java.MethodDeclarator;
import org.codehaus.janino.Java.MethodInvocation;
import org.codehaus.janino.Java.NewAnonymousClassInstance;
import org.codehaus.janino.Java.NewArray;
import org.codehaus.janino.Java.NewClassInstance;
import org.codehaus.janino.Java.NewInitializedArray;
import org.codehaus.janino.Java.NullLiteral;
import org.codehaus.janino.Java.Package;
import org.codehaus.janino.Java.ParameterAccess;
import org.codehaus.janino.Java.ParenthesizedExpression;
import org.codehaus.janino.Java.QualifiedThisReference;
import org.codehaus.janino.Java.ReferenceType;
import org.codehaus.janino.Java.ReturnStatement;
import org.codehaus.janino.Java.RvalueMemberType;
import org.codehaus.janino.Java.SimpleConstant;
import org.codehaus.janino.Java.SimpleType;
import org.codehaus.janino.Java.StringLiteral;
import org.codehaus.janino.Java.SuperConstructorInvocation;
import org.codehaus.janino.Java.SuperclassFieldAccessExpression;
import org.codehaus.janino.Java.SuperclassMethodInvocation;
import org.codehaus.janino.Java.SwitchStatement;
import org.codehaus.janino.Java.SynchronizedStatement;
import org.codehaus.janino.Java.ThisReference;
import org.codehaus.janino.Java.ThrowStatement;
import org.codehaus.janino.Java.TryStatement;
import org.codehaus.janino.Java.UnaryOperation;
import org.codehaus.janino.Java.WhileStatement;
import org.codehaus.janino.Visitor.AtomVisitor;
import org.codehaus.janino.Visitor.BlockStatementVisitor;
import org.codehaus.janino.Visitor.LvalueVisitor;
import org.codehaus.janino.Visitor.RvalueVisitor;
import org.codehaus.janino.util.Traverser;

import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.CodeRepr.CodeAndLine;

public class WorkflowTraverserAnalysis extends Traverser {

	private static Map<String, CodeRepr> bodies = new HashMap<>();
	
	public static Map<String, CodeRepr> getWorkflowBody(Java.CompilationUnit cu){
		WorkflowTraverserAnalysis wea = new WorkflowTraverserAnalysis();
		wea.traverseCompilationUnit(cu);
		return bodies;
	}

	@Override
	public void traverseMethodDeclarator(MethodDeclarator md){
		BlockExplorer be = new BlockExplorer();
		List<? extends BlockStatement> methodBody = md.optionalStatements;
		for(BlockStatement bs : methodBody){
			bs.accept(be);
		}
//		List<CodeRepr.CodeAndLine> code = be.getCode();
		bodies.put(md.toString(), new CodeRepr(be.getCode()));
	}
	
	class BlockExplorer implements BlockStatementVisitor{
		
		private List<CodeRepr.CodeAndLine> code;
		private RVExplorer rve;
		
		public BlockExplorer(){
			code = new ArrayList<>();
			rve = new RVExplorer(code);
		}
		
		public List<CodeAndLine> getCode(){
			return code;
		}
		
		@Override
		public void visitInitializer(Initializer i) {
			String init = i.toString();
			int line = i.getLocation().getLineNumber();
			code.add(new CodeRepr().new CodeAndLine(init, line));
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
			int line = es.getLocation().getLineNumber();
			code.add(new CodeRepr().new CodeAndLine(expression, line));
		}

		@Override
		public void visitIfStatement(IfStatement is) {
			// Main if is mandatory!
			int line = is.getLocation().getLineNumber();
			String _if = "if(";
			code.add(new CodeRepr().new CodeAndLine(_if, line));
			String condition = is.condition.toString();
			code.add(new CodeRepr().new CodeAndLine(condition, line));
			String if_ = ") {";
			code.add(new CodeRepr().new CodeAndLine(if_, line));
			(is.thenStatement).accept(this);
			code.add(new CodeRepr().new CodeAndLine("}", line));
			if (is.optionalElseStatement != null){
				String _else = "else {";
				code.add(new CodeRepr().new CodeAndLine(_else, is.optionalElseStatement.getLocation().getLineNumber()));
				(is.optionalElseStatement).accept(this);
				code.add(new CodeRepr().new CodeAndLine("}", is.optionalElseStatement.getLocation().getLineNumber()));
			}
		}

		@Override
		public void visitForStatement(ForStatement fs) {
			int line = fs.getLocation().getLineNumber();
			String _for = "for(";
			code.add(new CodeRepr().new CodeAndLine(_for, line));
			(fs.optionalInit).accept(this);
			String condition = fs.optionalCondition.toString() + ";";
			// FIXME: this should be the right way to proceed
//			(fs.optionalCondition).accept(rve);
			code.add(new CodeRepr().new CodeAndLine(condition, line));
			String update = (fs.optionalUpdate).toString() + ")";
			// FIXME: this should be the right way to proceed
//			(fs.optionalCondition).accept(rve);
			code.add(new CodeRepr().new CodeAndLine(update, line));
			String for_ = ") {";
			code.add(new CodeRepr().new CodeAndLine(for_, line));
			(fs.body).accept(this);
			String closefor = "}";
			code.add(new CodeRepr().new CodeAndLine(closefor, line));
		}
		
		@Override
		public void visitLocalVariableDeclarationStatement(LocalVariableDeclarationStatement lvds) {
			String lvDeclaration = lvds.toString();
			int line = lvds.getLocation().getLineNumber();
			code.add(new CodeRepr().new CodeAndLine(lvDeclaration, line));
		}

		@Override
		public void visitReturnStatement(ReturnStatement rs) {
			String returnStatement = rs.toString();
			int line = rs.getLocation().getLineNumber();
			code.add(new CodeRepr().new CodeAndLine(returnStatement, line));
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
	
	class RVExplorer implements RvalueVisitor, LvalueVisitor, AtomVisitor {

		private List<CodeAndLine> code;
		
		public RVExplorer(List<CodeAndLine> code){
			this.code = code;
		}
		
		@Override
		public void visitBinaryOperation(BinaryOperation bo) {
			int line = bo.getLocation().getLineNumber();
			(bo.lhs).accept(this);
			String operator = bo.op;
			code.add(new CodeRepr().new CodeAndLine(operator, line));
			(bo.rhs).accept(this);
		}
		
		@Override
		public void visitAssignment(Assignment a) {
			int line = a.getLocation().getLineNumber();
			(a.lhs).accept(this);
			String operator = a.operator;
			code.add(new CodeRepr().new CodeAndLine(operator, line));
			(a.rhs).accept(this);
		}
		
		@Override
		public void visitLocalVariableAccess(LocalVariableAccess lva) {
			int line = lva.getLocation().getLineNumber();
			String variable = lva.localVariable.toString();
			code.add(new CodeRepr().new CodeAndLine(variable, line));
		}
		
		@Override
		public void visitCrement(Crement c) {
//			String operator = c.operator;
//			String value = c.operand.toString();
//			if(c.pre){
//				code.add(operator+value);
//			}
		}
		
		@Override
		public void visitAmbiguousName(AmbiguousName an) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitArrayAccessExpression(ArrayAccessExpression aae) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitFieldAccess(FieldAccess fa) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitFieldAccessExpression(FieldAccessExpression fae) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitSuperclassFieldAccessExpression(
				SuperclassFieldAccessExpression scfae) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitParenthesizedExpression(ParenthesizedExpression pe) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitArrayLength(ArrayLength al) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitUnaryOperation(UnaryOperation uo) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitCast(Cast c) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitClassLiteral(ClassLiteral cl) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitConditionalExpression(ConditionalExpression ce) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitInstanceof(Instanceof io) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitMethodInvocation(MethodInvocation mi) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitSuperclassMethodInvocation(
				SuperclassMethodInvocation smi) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitIntegerLiteral(IntegerLiteral il) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitFloatingPointLiteral(FloatingPointLiteral fpl) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitBooleanLiteral(BooleanLiteral bl) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitCharacterLiteral(CharacterLiteral cl) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitStringLiteral(StringLiteral sl) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitNullLiteral(NullLiteral nl) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitSimpleConstant(SimpleConstant sl) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitNewAnonymousClassInstance(
				NewAnonymousClassInstance naci) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitNewArray(NewArray na) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitNewInitializedArray(NewInitializedArray nia) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitNewClassInstance(NewClassInstance nci) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitParameterAccess(ParameterAccess pa) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitQualifiedThisReference(QualifiedThisReference arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitThisReference(ThisReference tr) {
			// TODO Auto-generated method stub
			
		}
		
		/** AtomVisitor interface **/

		@Override
		public void visitArrayType(ArrayType at) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitBasicType(BasicType bt) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitReferenceType(ReferenceType rt) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitRvalueMemberType(RvalueMemberType rmt) {
			String identifier = rmt.identifier;
			int line = rmt.getLocation().getLineNumber();
			code.add(new CodeRepr().new CodeAndLine(identifier, line));
		}

		@Override
		public void visitSimpleType(SimpleType st) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void visitPackage(Package p) {
			// TODO Auto-generated method stub
			
		}
	}
	
}