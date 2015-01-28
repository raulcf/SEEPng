package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class InvalidQueryDefinitionException extends SeepException {

	private static final long serialVersionUID = 1L;

	public InvalidQueryDefinitionException(String msg){
		super(msg);
	}
	
}
