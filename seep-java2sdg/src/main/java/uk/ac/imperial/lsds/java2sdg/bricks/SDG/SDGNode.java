package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import java.util.Map;

public class SDGNode {

	private int id = 0;
	private String name;
	private Map<Integer, TaskElementRepr> taskElements;
	private StateElementRepr stateElement;
	
	public SDGNode(String name, Map<Integer, TaskElementRepr> taskElements, StateElementRepr stateElement) {
		this.id = id++;
		this.name = name;
		this.taskElements = taskElements;
		this.stateElement = stateElement;
	}
	
}
