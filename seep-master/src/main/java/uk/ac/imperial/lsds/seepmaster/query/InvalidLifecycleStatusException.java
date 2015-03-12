package uk.ac.imperial.lsds.seepmaster.query;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class InvalidLifecycleStatusException extends SeepException {

	private static final long serialVersionUID = 1L;

	public InvalidLifecycleStatusException(String msg){
		super(msg);
	}
	
}
