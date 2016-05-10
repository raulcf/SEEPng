package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.codehaus.janino.Java;
import org.codehaus.janino.Java.Annotation;
import org.codehaus.janino.Java.Atom;
import org.codehaus.janino.Java.CompilationUnit;
import org.codehaus.janino.Java.LocalVariableDeclarationStatement;
import org.codehaus.janino.Java.VariableDeclarator;
import org.codehaus.janino.util.Traverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.janino.Java.Type;

import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;
import uk.ac.imperial.lsds.java2sdg.bricks.VariableRepr;

public class LiveVariableAnalysis extends Traverser {

	private final static Logger LOG = LoggerFactory.getLogger(LiveVariableAnalysis.class.getSimpleName());
	
	private HashMap<String, VariableLivenessInformation> lvData;
	private LivenessInformation lvInfo;
	
	private Java.MethodDeclarator currMethod = null;
	
	public LiveVariableAnalysis(){
		this.lvData = new HashMap<>();
	}
	
	public static LivenessInformation getLVInfo(CompilationUnit cu){
		LiveVariableAnalysis lva = new LiveVariableAnalysis();
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
	public void traverseMethodDeclarator(Java.MethodDeclarator md){
		currMethod = md;
		super.traverseMethodDeclarator(md);
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
		VariableLivenessInformation li = new VariableLivenessInformation(varName, currMethod.name, line, t);
		lvData.put(varName, li);
	}
	
	private void lastSeen(String varName, int line){
		if(lvData.containsKey(varName)){
			if(lvData.get(varName).getMethodName().compareTo(currMethod.name)==0)
				lvData.get(varName).updateLivesTo(varName, line);
			else{
				LOG.error("Attempt to overwritte local Var {} - Method {} -> New method {} ",
						varName, lvData.get(varName).getMethodName(), currMethod.name);
			}
		}
	}
	
	class VariableLivenessInformation{
		
		private final String localVarName;
		private final String methodName;
		//Janino variable Type
		private final Type localVarType;
		private final int livesFrom;
		private int livesTo = 0;
		
		public VariableLivenessInformation(String varName, String methodName, int line, Type t){
			this.localVarName = varName;
			this.methodName = methodName;
			this.localVarType = t;
			this.livesFrom = line;
			this.livesTo = line;
		}
		
		public String getMethodName() {
			return methodName;
		}

		public int getLivesFrom(){
			return this.livesFrom;
		}
		
		public int getLivesTo(){
			return this.livesTo;
		}
		
		public Type getType(){
			return this.localVarType;
		}
		
		public void updateLivesTo(String varName, int line){
			if(this.localVarName.equals(varName)){
				this.livesTo = line;
				LOG.debug(varName+" livesTo "+line);
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
		private HashMap<String, VariableLivenessInformation> lvData;
		
		public LivenessInformation(HashMap<String, VariableLivenessInformation> lvData){
			this.lvData = lvData;
		}
		
		public List<VariableRepr> getLiveInputVarsAt(String methodName, int line){
			LOG.debug("getInputVars at Line: "+ line +" Method: "+ methodName);
			List<VariableRepr> toreturn = new ArrayList<VariableRepr>();
			for(Map.Entry<String, VariableLivenessInformation> entry: lvData.entrySet() ){
				if( (entry.getValue().getLivesFrom() < line) && (methodName.contains(entry.getValue().getMethodName()))){
					toreturn.add(VariableRepr.var(entry.getValue().getType(), entry.getKey()));
				}
			}
			return toreturn;
		}
		
		public List<VariableRepr> getLiveOutputVarsAt(String methodName, int line){
			LOG.debug("getOutputVars at line: "+ line + " Method: "+ methodName);
			List<VariableRepr> toreturn = new ArrayList<VariableRepr>();
			for(Map.Entry<String, VariableLivenessInformation> entry: lvData.entrySet() ){
				if( (entry.getValue().getLivesTo() > line) && (methodName.contains(entry.getValue().getMethodName()))){
					toreturn.add(VariableRepr.var(entry.getValue().getType(), entry.getKey()));
				}
			}
			return toreturn;
		}
		
	}
}
