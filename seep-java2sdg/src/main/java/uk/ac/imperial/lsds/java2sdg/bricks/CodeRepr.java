package uk.ac.imperial.lsds.java2sdg.bricks;

import java.util.ArrayList;
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
	
	public List<String> getCodeText(){
		List<String> toreturn = new ArrayList<String>();
		for(CodeAndLine c : code)
			toreturn.add(c.code);
		return toreturn;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("CodeRepr: \n");
		for(CodeAndLine c : code)
			sb.append("\t line: "+c.line + " \t code: "+ c.code +"\n");
		return sb.toString();
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
