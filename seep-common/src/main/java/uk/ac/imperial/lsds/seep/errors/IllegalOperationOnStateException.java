package uk.ac.imperial.lsds.seep.errors;

public class IllegalOperationOnStateException extends SeepException {

	private static final long serialVersionUID = 1L;

	public IllegalOperationOnStateException(String msg){
		super(msg);
	}
	
}
