package uk.ac.imperial.lsds.seep.api.state.stateimpl;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class IllegalOperationOnStateException extends SeepException {

	private static final long serialVersionUID = 1L;

	public IllegalOperationOnStateException(String msg){
		super(msg);
	}
	
}
