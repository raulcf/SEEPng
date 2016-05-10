package api;

import api.topology.Cluster;

/**
 * Gives access to the API that is used within the program() function
 * @author ra-mit
 *
 */
public interface SeepProgram {
	
	final public API api = new APIImplementation();
	
	public void configure(Cluster c);
	public void program();
	
}
