package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

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
		SDGNode s2 = new SDGNode("fake2", taskElements, null);
		List<SDGNode> sdgNodes = new ArrayList<>();
		sdgNodes.add(s);
		sdgNodes.add(s2);
		
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
