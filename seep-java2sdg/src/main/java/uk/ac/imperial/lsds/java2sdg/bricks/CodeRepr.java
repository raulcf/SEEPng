package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.List;

public class CodeRepr {

	private List<CodeAndLine> code;
	
	public CodeRepr(){ }
	
	public CodeRepr(List<CodeAndLine> codeLines) {
		this.code = codeLines;
	}
	
	public int getInitLine(){
		return code.get(0).line;
	}
	
	public int getEndLine(){
		return code.get(code.size()-1).line;
	}
	
	public class CodeAndLine {

		public final String code;
		public final int line;
		
		public CodeAndLine(String code, int line){
			this.code = code;
			this.line = line;
		}
	}
	
}
