package uk.ac.imperial.lsds.seep.api.state;

/**
 * A class that implements Versionable indicates that it can create a version of itself to allow, e.g. concurrent access.
 * It must implement a reconcile method to indicate how to reconcile both versions.
 * @author ra
 *
 */
public interface Versionable {

	public void enterSnapshotMode();
	public void reconcile();
	
}
