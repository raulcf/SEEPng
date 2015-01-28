package uk.ac.imperial.lsds.seep.api.state;

/**
 * A class implements SeepState to indicate that it is state visible to SEEPng and that this state has some SeepTask owner.
 * @author ra
 *
 */
public interface SeepState {

	public void setOwner(int owner);
	public int getOwner();
	
}
