package uk.ac.imperial.lsds.java2sdg.errors;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class LimitationException extends SeepException {

	private static final long serialVersionUID = 1L;

	public LimitationException(String msg){
		super(msg);
	}
	
}
