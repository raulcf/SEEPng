package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

public class StateElement {
	
	private static int stateCount=0;
	
	private int stateId;
	private String stateName;
	
	public static StateElement createStateElementRepr(String name){
		return new StateElement(++StateElement.stateCount, name);
	}
	
	private StateElement(int id, String name){
		this.stateId = id;
		this.stateName = name;
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
		return stateName;
	}

	/**
	 * @param stateName the stateName to set
	 */
	public void setStateName(String stateName) {
		this.stateName = stateName;
	}

}
