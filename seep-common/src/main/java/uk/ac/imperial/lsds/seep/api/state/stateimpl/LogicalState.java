package uk.ac.imperial.lsds.seep.api.state.stateimpl;

import java.util.Map;

import uk.ac.imperial.lsds.seep.api.state.SeepState;

public class LogicalState implements SeepState {

	private int owner;
	private Map<String, SeepState> states;
	
	public LogicalState(){}
	public LogicalState(Map<String, SeepState> states){
		this.states = states;
	}
	
	public SeepState get(String name){
		return states.get(name);
	}

	@Override
	public void setOwner(int owner) {
		this.owner = owner;
	}

	@Override
	public int getOwner() {
		return owner;
	}
	
}
