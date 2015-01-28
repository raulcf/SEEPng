package uk.ac.imperial.lsds.seep.api.state;

import java.util.Iterator;

/**
 * A class implements Streamable to indicate that its content can be streamed. In particular, it can be accessed through
 * an iterator that will provide access to chunks, split in an application-dependent fashion.
 * @author ra
 *
 */
public interface Streamable {

	public Iterator<? extends Object> makeStream();
	
}
