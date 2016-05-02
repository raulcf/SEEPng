package uk.ac.imperial.lsds.java2sdg.bricks.sdg;

public class StateElementRepr {
	
	private static int stateCount=0;
	
	private int stateId;
	private String stateName;
	
	public static StateElementRepr createStateElementRepr(String name){
		return new StateElementRepr(++StateElementRepr.stateCount, name);
	}
	
	private StateElementRepr(int id, String name){
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
