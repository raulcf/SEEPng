package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

import uk.ac.imperial.lsds.java2sdg.bricks.InternalStateRepr;

public class StateElement {
	
	private static int stateCount=0;
	
	private int stateId;
	private InternalStateRepr stateRepr;
	

	public static StateElement createStateElementRepr(InternalStateRepr stateRepr){
		return new StateElement(++StateElement.stateCount, stateRepr);
	}
	
	private StateElement(int id, InternalStateRepr stateRepr){
		this.stateId = id;
		this.stateRepr = stateRepr;
	}

	/**
	 * @return the stateId
	 */
	public int getStateId() {
		return stateId;
	}

	/**
	 * @param stateId the stateId to set
	 */
	public void setStateId(int stateId) {
		this.stateId = stateId;
	}

	/**
	 * @return the stateName
	 */
	public String getStateName() {
		return this.getStateRepr().getName();
	}

	/**
	 * @return the stateRepr
	 */
	public InternalStateRepr getStateRepr() {
		return stateRepr;
	}
	
}
