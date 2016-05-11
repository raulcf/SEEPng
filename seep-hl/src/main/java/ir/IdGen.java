package ir;

/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
public class IdGen {

	private static IdGen instance;
	
	private int id = 0;
	
	private IdGen() { }
	
	public static IdGen getInstance() {
		if(instance == null) {
			instance = new IdGen();
		}
		return instance;
	}
	
	public int id() {
		id++;
		return id;
	}
}
