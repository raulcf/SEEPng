package api;

/**
 * Gives access to the API that is used within the program() function
 * @author ra-mit
 *
 */
public interface SeepProgram {
	
	final public API api = new APIImplementation();
	
	public void program();
	
}
