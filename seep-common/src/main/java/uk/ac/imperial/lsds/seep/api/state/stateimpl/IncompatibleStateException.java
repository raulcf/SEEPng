package uk.ac.imperial.lsds.seep.api.state.stateimpl;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class IncompatibleStateException extends SeepException {

	private static final long serialVersionUID = 1L;

	public IncompatibleStateException(String msg){
		super(msg);
	}
}
