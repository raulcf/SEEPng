package uk.ac.imperial.lsds.seep.api.state;

import java.util.List;

/**
 * A class implements Mergeable to indicate that it can accept more instances of itself to be merged. The particular
 * merge strategy will depend on the implementation, e.g. it may mean that the new instances overwrite the current instance,
 * that they do not overwrite, that instances are averaged or aggregated according to any other application-specific 
 * logic, etc.
 * @author ra
 *
 */
public interface Mergeable {

	public void merge(List<SeepState> state);
	
}
