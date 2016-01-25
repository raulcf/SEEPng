package uk.ac.imperial.lsds.seep.errors;

public class DeadlockException extends SeepException {

	private static final long serialVersionUID = 1L;

	public DeadlockException(String msg){
		super(msg);
	}
}
