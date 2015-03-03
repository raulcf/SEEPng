package uk.ac.imperial.lsds.java2sdg.analysis;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.janino.Java.Atom;
import org.codehaus.janino.Java.CompilationUnit;
import org.codehaus.janino.Java.LocalVariableDeclarationStatement;
import org.codehaus.janino.Java.VariableDeclarator;
import org.codehaus.janino.util.Traverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LVAnalysis extends Traverser {

	private final static Logger LOG = LoggerFactory.getLogger(LVAnalysis.class.getSimpleName());
	
	private HashMap<String, LivenessInformation> lvData;
	
	public LVAnalysis(){
		this.lvData = new HashMap<>();
	}
	
	public static Map<String, LivenessInformation> getLVInfo(CompilationUnit cu){
		LVAnalysis lva = new LVAnalysis();
		lva.traverseCompilationUnit(cu);
		return lva.lvData;
	}
	
	@Override 
    public void traverseLocalVariableDeclarationStatement(LocalVariableDeclarationStatement lvds) {
		VariableDeclarator[] varDeclarators = lvds.variableDeclarators;
		for(VariableDeclarator vd : varDeclarators){
			String variableName = vd.name;
			int line = lvds.getLocation().getLineNumber();
			newVariableDeclaration(variableName, line);
		}
	}
	
	@Override
	public void traverseAtom(Atom a){
		String atomName = a.toString();
		int line = a.getLocation().getLineNumber();
		lastSeen(atomName, line);
	}
	
	private void newVariableDeclaration(String varName, int line){
		if(lvData.containsKey(varName)){
			LOG.error("already registered variable. overwriting info?");
		}
		LivenessInformation li = new LivenessInformation(varName, line);
		lvData.put(varName, li);
	}
	
	private void lastSeen(String varName, int line){
		if(lvData.containsKey(varName)){
			lvData.get(varName).updateLivesTo(varName, line);
		}
	}
	
	public class LivenessInformation{
		
		private final String varName;
		private final int livesFrom;
		private int livesTo = 0;
		
		public LivenessInformation(String varName, int line){
			this.varName = varName;
			this.livesFrom = line;
			this.livesTo = line;
		}
		
		public int getLivesFrom(){
			return livesFrom;
		}
		
		public int getLivesTo(){
			return livesTo;
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
			return line <= livesTo;
		}
		
	}
}
