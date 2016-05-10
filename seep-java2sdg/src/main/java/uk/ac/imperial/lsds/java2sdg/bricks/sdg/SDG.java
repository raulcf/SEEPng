package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;

public class SDG {

	// Nodes in the SDG
	private List<SDGNode> sdgNodes;

	public SDG(List<SDGNode> nodes) {
		this.sdgNodes = nodes;
	}
	
	public static SDG createSDGFromPartialSDG(List<PartialSDGRepr> partialSDGs){
		List<SDGNode> sdgNodes = new ArrayList<>();
		int partialID = 0;
		
		for ( PartialSDGRepr partial : partialSDGs ){

			//Checking if Partial SDG has Source and add as separate node
			Map<Integer, TaskElement> taskElements = new HashMap<>();
			if(partial.getSource() != null){
				TaskElement src = new TaskElement(0);
				src.setOutputSchema(partial.getSource().getSchema());
				src.setSouce(true);
				//create connection with the first TaskElement
				src.setDownstreams(Arrays.asList(partial.getTEs().get(0).getId()));
				taskElements.put(0, src);
				SDGNode srcNode = new SDGNode("source_"+partialID, taskElements, null);
				sdgNodes.add(srcNode);
				partialID++;
			}
			
			//Create ONE SDGNode per partialSDG
			taskElements = new HashMap<>();
			TaskElement lastTE = null;
			for(TaskElement el : partial.getTEs()) {
				taskElements.put(el.getId(), el);
				lastTE = el;
			}
			//Use just Method name (no arguments)
			String wfName = partial.getWorkflowName().substring(0, partial.getWorkflowName().indexOf("("))+"_"+partialID;
			SDGNode s = new SDGNode(wfName, taskElements, null);
			sdgNodes.add(s);
			partialID++;
			
			//Checking if Partial SDG has SINK and add as separate node
			if(partial.getSink() != null){
				//connect Last TE with Sink
				lastTE.setDownstreams(Arrays.asList(lastTE.getId()+1));
				lastTE.setOutputSchema(partial.getSink().getSchema());
				//Create TE Node
				TaskElement snk = new TaskElement(lastTE.getId()+1);
				snk.setSink(true);
				taskElements = new HashMap<>();
				taskElements.put(lastTE.getId()+1, snk);
				SDGNode snkNode = new SDGNode("sink_"+partialID, taskElements, null);
				sdgNodes.add(snkNode);
				partialID++;
			}
		}
		
		SDG sdg = new SDG(sdgNodes);
		
		return sdg;
	}
	
	/**
	 * @return the sdgNodes
	 */
	public List<SDGNode> getSdgNodes() {
		return sdgNodes;
	}
	
}
