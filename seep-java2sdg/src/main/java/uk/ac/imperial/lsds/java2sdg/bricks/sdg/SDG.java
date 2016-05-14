package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.janino.Java.BasicType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.PartialSDGRepr;
import uk.ac.imperial.lsds.java2sdg.bricks.SDGAnnotation;
import uk.ac.imperial.lsds.java2sdg.bricks.VariableRepr;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.errors.SchemaException;

public class SDG {

	// Nodes in the SDG
	private static List<SDGNode> sdgNodes;
	private final static Logger LOG = LoggerFactory.getLogger(SDG.class.getCanonicalName());
	
	public SDG(List<SDGNode> nodes) {
		sdgNodes = nodes;
	}
	
	public static SDG createSDGFromPartialSDG(List<PartialSDGRepr> partialSDGs, Map<String, InternalStateRepr> stateFields){
		sdgNodes = new ArrayList<>();
		int partialID = 0;
		
		for ( PartialSDGRepr partial : partialSDGs ){

			
			Map<Integer, TaskElement> taskElements = new HashMap<>();
			//Checking if Partial SDG has Source and add as separate node
			if(partial.getSource() != null){
				TaskElement src = new TaskElement(0);
				src.setOutputStore(partial.getSource());
				src.setSouce(true);
				//create connection with the first TaskElement
				src.setDownstreams(Arrays.asList(partial.getTEs().get(0).getId()));
				taskElements.put(0, src);
				SDGNode srcNode = new SDGNode("source_"+partialID, taskElements, null);
				sdgNodes.add(srcNode);
				partialID++;
			}
			
			taskElements = new HashMap<>();
			//Create ONE SDGNode per partialSDG
			TaskElement lastTE = null;
			for(TaskElement el : partial.getTEs()) {
				taskElements.put(el.getId(), el);
				lastTE = el;
			}
			//Use just Method name (no arguments)
			String wfName = partial.getWorkflowName().substring(0, partial.getWorkflowName().indexOf("("))+"_"+partialID;
			SDGNode s = new SDGNode(wfName, taskElements, discoverSDGNodeState(taskElements, stateFields));	
			if(!isSDGNodeMergable(s)){
				sdgNodes.add(s);
				partialID++;
			}
			else{
				//If last merged partial created a Source -remove it
				if(partial.getSource() != null)
					sdgNodes.remove(sdgNodes.size()-1);
				//skip the rest of the code below
				continue;
			}
			
			
			//Checking if Partial SDG has SINK and add as separate node
			if(partial.getSink() != null){
				//connect Last TE with Sink
				lastTE.setDownstreams(Arrays.asList(lastTE.getId()+1));
				lastTE.setOutputStore(partial.getSink());
				//Create TE Node
				TaskElement snk = new TaskElement(sdgNodes.size()+1);
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
	 * If two partial SDGs share the same PARTITIONED state merges them in a single partial SDG
	 * The final Partial SDG contains a superset of the Schema will all the needed variables
	 * 
	 * @param node
	 * @return true if mergeable / false otherwise
	 */
	public static boolean isSDGNodeMergable(SDGNode currentNode){
		if(currentNode.getStateElement() == null || 
				currentNode.getStateElement().getStateRepr().getStateAnnotation() != SDGAnnotation.PARTITIONED )
			return false;
		//Always contains one TE!
		int currentTEid = currentNode.getTaskElements().entrySet().iterator().next().getKey();
		TaskElement currentTE = currentNode.getTaskElements().entrySet().iterator().next().getValue();
		
		for(SDGNode n : sdgNodes){
			
			if(n.isSource() || n.isSink())
				continue;
			//Nodes can be merged case!
			if( (n.getStateElement()!= null) && 
					(n.getStateElement().getStateName().equals(currentNode.getStateElement().getStateRepr().getName()))){
					
					//Create Multi Schema
					Schema multiSchema = createMultiBranchSchema(currentTE.getOutputSchema(), n.getTaskElements().values().iterator().next().getOutputSchema());
					DataStore multiDataStore = new DataStore(multiSchema, currentTE.getOutputStore().type(), currentTE.getOutputStore().getConfig());
					
					//Set new DataStore
					currentTE.setOutputStore(multiDataStore);
					n.getTaskElements().values().iterator().next().setOutputStore(multiDataStore);
					//Set extra variable (branchId) to stream 
					currentTE.getOutputVariables().add(VariableRepr.var(new BasicType(null, BasicType.INT), "branchId"));
					n.getTaskElements().values().iterator().next().getOutputVariables().add(VariableRepr.var(new BasicType(null, BasicType.INT), "branchId"));
					
					//Also change existing source Schema
					for(SDGNode node: sdgNodes){
						if(node.isSource())
							node.getTaskElements().values().iterator().next().setOutputStore(multiDataStore);
					}
					n.getTaskElements().put(currentTEid, currentTE);
				return true;
			}
		}
		
		return false;
	}
	
	public static Schema createMultiBranchSchema(Schema one, Schema other){
		SchemaBuilder builder = SchemaBuilder.getInstance();
		
		for(int i =0; i < one.names().length; i++){
			builder.newField(one.getField(one.names()[i]), one.names()[i]);
		}
		try{
			for(int i =0; i < other.names().length; i++){
				builder.newField(other.getField(other.names()[i]), other.names()[i]);
			}
		}catch (SchemaException ex){
			LOG.info("Multi Schema Field already exists!");
		}
		builder.newField(Type.INT, "branchId");
		
		return builder.build();
	}
	
	
	public static StateElement discoverSDGNodeState(Map<Integer, TaskElement> taskElements, Map<String, InternalStateRepr> stateFields){
		for(String stateName: stateFields.keySet()){
			for(TaskElement task : taskElements.values()){
				for(String c : task.getCode()){
					if(c.contains(stateName))
						return StateElement.createStateElementRepr(stateFields.get(stateName));
				}
			}
		}
		
		return null;
	}
	
	
	/**
	 * @return the sdgNodes
	 */
	public List<SDGNode> getSdgNodes() {
		return sdgNodes;
	}
	
}
