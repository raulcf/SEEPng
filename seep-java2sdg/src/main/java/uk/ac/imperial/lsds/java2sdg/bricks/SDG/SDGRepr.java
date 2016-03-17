package uk.ac.imperial.lsds.java2sdg.bricks.SDG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;

public class SDGRepr {

	// Nodes in the SDG
	private List<SDGNode> sdgNodes;
	
	public SDGRepr(List<SDGNode> nodes) {
		this.sdgNodes = nodes;
	}
	
	public static SDGRepr createSDGFromPartialSDG(List<PartialSDGRepr> partialSDGs){
		
		PartialSDGRepr fake = partialSDGs.iterator().next();
		Map<Integer, TaskElementRepr> taskElements = new HashMap<>();
		for(TaskElementRepr el : fake.getTEs()) {
			taskElements.put(el.getId(), el);
		}
		SDGNode s = new SDGNode("fake", taskElements, null);
		List<SDGNode> sdgNodes = new ArrayList<>();
		sdgNodes.add(s);
		
		SDGRepr sdg = new SDGRepr(sdgNodes);
		
		return sdg;
	}
	
}
