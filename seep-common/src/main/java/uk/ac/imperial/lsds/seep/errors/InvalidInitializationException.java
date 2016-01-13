package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class InvalidInitializationException extends SeepException {

	private static final long serialVersionUID = 1L;
	
	public InvalidInitializationException(String msg) {
		super(msg);
	}

}
