package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ir.Traceable;
import ir.Traceable.TraceableType;

public class DOTExporter {
	
	private String repr;
	
	final String shBox = "shape=box";
	final String label = "label=";
	
	public void export(Map<Integer, Traceable> traces) {
		
		Map<Integer, Traceable> flatTraces = new HashMap<>();
		List<Traceable> trac = new ArrayList<>();
		for(Traceable t : traces.values()) {
			trac.add(t);
		}
		flatTraces = flattenNestedTraces(trac, flatTraces);
		
		String formattedNodes = formatAllNodes(flatTraces);
		
		String connectedNodes = connectAllNodes(flatTraces);
		
		System.out.println(connectedNodes);
		
		StringBuffer sb = new StringBuffer();
		sb.append("digraph dataTaskOutput {");
		sb.append(System.lineSeparator());
		sb.append(formattedNodes);
		sb.append(System.lineSeparator());
		sb.append(connectedNodes);
		sb.append(System.lineSeparator());
		sb.append("}");
		
		this.repr = sb.toString();
	}
	
	private String connectAllNodes(Map<Integer, Traceable> traces) {
		Set<Integer> included = new HashSet<>();
		StringBuffer sb = new StringBuffer();
		for(Traceable t : traces.values()) {
			int id = t.getId();
			if(included.contains(id)) continue; // include only once
			included.add(id);
			StringBuffer in = new StringBuffer();
			for(Traceable ot : t.getOutput()) {
				in.append(id +"->"+ot.getId()+";");
				in.append(System.lineSeparator());
			}
			sb.append(in.toString());
			sb.append(System.lineSeparator());
		}
		return sb.toString();
	}
	
	private String formatAllNodes(Map<Integer, Traceable> traces) {
		StringBuffer sb = new StringBuffer();
		for(Traceable t : traces.values()) {
			int id = t.getId();
			TraceableType tt = t.getTraceableType();
			String name = t.getName();
			String format = null;
			String lbl = this.label+"\""+id+"_"+name+"\"";
			if(tt.equals(TraceableType.TASK)) {
				format = id + "["+shBox+" "+lbl+"];";
			}
			else {
				format = id + "["+lbl+"];";
			}
			sb.append(format);
			sb.append(System.lineSeparator());
		}
		return sb.toString();
	}

	public String getStringRepr() {
		return repr;
	}
	
	private Map<Integer, Traceable> flattenNestedTraces(List<Traceable> traces, Map<Integer, Traceable> flat) {
		for(Traceable t : traces) {
			flat.put(t.getId(), t);
			flattenNestedTraces(t.getOutput(), flat);
		}
		return flat;
	}

}
