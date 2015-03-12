package uk.ac.imperial.lsds.java2sdg.output;

public enum OutputTarget {
	DOT(0),
	GEXF(1),
	X_JAR(2);
	
	private int id;
	
	OutputTarget(int id){
		this.id = id;
	}
	
	public static OutputTarget ofType(int id){
		for(OutputTarget ot : OutputTarget.values()){
			if(ot.id == id) return ot;
		}
		return null;
	}
}
