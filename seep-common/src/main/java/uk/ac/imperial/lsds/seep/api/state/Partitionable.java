package uk.ac.imperial.lsds.seep.api.state;

import java.util.List;

/**
 * A class implements Partitionable to indicate that it can be split into different partitions according to some
 * application-dependent logic.
 * @author ra
 *
 */
public interface Partitionable {

	public List<? extends SeepState> partition();
	public List<? extends SeepState> partition(int partitions);
	
}
