package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import org.codehaus.janino.Java.Type;
import org.codehaus.janino.Java.BasicType;

public class SDGRepr {

	// Nodes in the SDG
	private List<SDGNode> sdgNodes;

	public SDGRepr(List<SDGNode> nodes) {
		this.sdgNodes = nodes;
	}
	
	public static SDGRepr createSDGFromPartialSDG(List<PartialSDGRepr> partialSDGs){
		List<SDGNode> sdgNodes = new ArrayList<>();
		int partialID = 0;
		
		for ( PartialSDGRepr partial : partialSDGs ){
			Map<Integer, TaskElementRepr> taskElements = new HashMap<>();
			for(TaskElementRepr el : partial.getTEs()) {
				/* Dummy line below - Janino Type Demo */
				//el.getOutputVariables().add(VariableRepr.var( new BasicType(null, BasicType.INT), "b"));
				taskElements.put(el.getId(), el);
				System.out.println("Task Element Code: "+ el.getCode());
			}
			SDGNode s = new SDGNode(String.valueOf(partialID), taskElements, null);
			
			sdgNodes.add(s);
			partialID++;
		}
		
		SDGRepr sdg = new SDGRepr(sdgNodes);
		
		return sdg;
	}
	
	/**
	 * @return the sdgNodes
	 */
	public List<SDGNode> getSdgNodes() {
		return sdgNodes;
	}
	
}
