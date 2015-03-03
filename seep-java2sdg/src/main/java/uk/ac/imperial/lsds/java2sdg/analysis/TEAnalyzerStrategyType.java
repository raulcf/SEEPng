package uk.ac.imperial.lsds.java2sdg.analysis;

public enum TEAnalyzerStrategyType {
	COARSE(0), 							// Simply one TE per workflow
	STATE_ACCESS(1);					// USENIX ATC 14 --- Making State Explicit for Imperative Big Data Processing
	
	private int type;
	
	TEAnalyzerStrategyType(int type){
		this.type = type;
	}
	
	public int ofType(){
		return this.type;
	}
	
	public static TEAnalyzerStrategyType getType(int type){
		for(TEAnalyzerStrategyType strategy : TEAnalyzerStrategyType.values()){
			if(strategy.type == type) return strategy;
		}
		return null;
	}
}
