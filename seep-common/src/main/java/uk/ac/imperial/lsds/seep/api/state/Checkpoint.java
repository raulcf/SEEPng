package uk.ac.imperial.lsds.seep.api.state;

/**
 * A class implement the Checkpoint interface to indicate that it can both serialize itself 
 * into a byte[] and deserialize a byte[] into an instance.
 * @author ra
 *
 */
public interface Checkpoint {

	public byte[] checkpoint();
	public void recover(byte[] bytes);
	
}
