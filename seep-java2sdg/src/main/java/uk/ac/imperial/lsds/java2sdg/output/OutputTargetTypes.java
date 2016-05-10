package uk.ac.imperial.lsds.java2sdg.output;

public enum OutputTargetTypes {
	DOT(0),
	GEXF(1),
	X_JAR(2);
	
	private int id;
	
	OutputTargetTypes(int id){
		this.id = id;
	}
	
	public static OutputTargetTypes ofType(int id){
		for(OutputTargetTypes ot : OutputTargetTypes.values()){
			if(ot.id == id) return ot;
		}
		return null;
	}
}
