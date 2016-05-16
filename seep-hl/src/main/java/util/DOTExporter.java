package util;

import java.util.Map;
import java.util.Set;

import ir.Traceable;

public class DOTExporter {

	private Set<Integer> includedNodes;
	
	private String repr;
	
	public void export(Map<Integer, Traceable> traces) {
		StringBuffer sb = new StringBuffer();
		sb.append("graph graphname {");
		sb.append(System.lineSeparator());
		sb.append("a -- b -- c;");
		sb.append(System.lineSeparator());
		sb.append("b -- d;");
		sb.append(System.lineSeparator());
		sb.append("}");
		
		this.repr = sb.toString();
	}

	public String getStringRepr() {
		return repr;
	}

}
