package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.codehaus.janino.Java.Atom;
import org.codehaus.janino.Java.CompilationUnit;
import org.codehaus.janino.Java.LocalVariableDeclarationStatement;
import org.codehaus.janino.Java.VariableDeclarator;
import org.codehaus.janino.util.Traverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.janino.Java.Type;
import uk.ac.imperial.lsds.java2sdg.bricks.sdg.VariableRepr;

public class LVAnalysis extends Traverser {

	private final static Logger LOG = LoggerFactory.getLogger(LVAnalysis.class.getSimpleName());
	
	private HashMap<String, VariableLivenessInformation> lvData;
	private LivenessInformation lvInfo;
	
	public LVAnalysis(){
		this.lvData = new HashMap<>();
	}
	
	public static LivenessInformation getLVInfo(CompilationUnit cu){
		LVAnalysis lva = new LVAnalysis();
		lva.traverseCompilationUnit(cu);
		lva.lvInfo = lva.buildLVInfo();
		return lva.lvInfo;
	}
	
	private LivenessInformation buildLVInfo(){
		// TODO: create lvinfo here with comfortable ifaces
		LivenessInformation lv = new LivenessInformation(lvData);
		
		return lv;
	}
	
	@Override 
    public void traverseLocalVariableDeclarationStatement(LocalVariableDeclarationStatement lvds) {
		VariableDeclarator[] varDeclarators = lvds.variableDeclarators;
		for(VariableDeclarator vd : varDeclarators){
			String variableName = vd.name;
			Type varType = lvds.type;
			int line = lvds.getLocation().getLineNumber();
			newVariableDeclaration(variableName, line, varType);
		}
	}
	
	/* Atom: Abstract base class for Java.Type, Java.Rvalue and Java.Lvalue.
	 * lvalue: i.e. an expression that has a type and a value, and can be assigned to ->
	 * An expression that can be the left-hand-side of an assignment.
	 */
	@Override
	public void traverseAtom(Atom a){
		String atomName = a.toString();
		int line = a.getLocation().getLineNumber();
		lastSeen(atomName, line);
	}
	
	private void newVariableDeclaration(String varName, int line, Type t){
		if(lvData.containsKey(varName)){
			LOG.error("already registered variable. overwriting info?");
		}
		VariableLivenessInformation li = new VariableLivenessInformation(varName, line, t);
		lvData.put(varName, li);
	}
	
	private void lastSeen(String varName, int line){
		if(lvData.containsKey(varName)){
			lvData.get(varName).updateLivesTo(varName, line);
		}
	}
	
	class VariableLivenessInformation{
		
		private final String varName;
		private final Type varType;
		private final int livesFrom;
		private int livesTo = 0;
		
		public VariableLivenessInformation(String varName, int line, Type t){
			this.varName = varName;
			this.varType = t;
			this.livesFrom = line;
			this.livesTo = line;
		}
		
		public int getLivesFrom(){
			return this.livesFrom;
		}
		
		public int getLivesTo(){
			return this.livesTo;
		}
		
		public Type getType(){
			return this.varType;
		}
		
		public void updateLivesTo(String varName, int line){
			if(this.varName.equals(varName)){
				this.livesTo = line;
				System.out.println(varName+" livesTo "+line);
			}
			else{
				LOG.error("Variable names do not MATCH!!");
			}
		}
		
		public boolean isLive(int line){
			return (line <= livesTo);
		}
		
	}
	
	public class LivenessInformation{
		HashMap<String, VariableLivenessInformation> lvData;
		
		public LivenessInformation(HashMap<String, VariableLivenessInformation> lvData){
			this.lvData = lvData;
		}
		
		//TODO: Add Implementation Logic and check correctness! => What about Variable type?
		// For now just return an empty List
		public List<VariableRepr> getLiveVarsAt(int line){
			List<VariableRepr> toreturn = new ArrayList<VariableRepr>();
			return toreturn;
		}
	}
}
