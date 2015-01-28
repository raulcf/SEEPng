package uk.ac.imperial.lsds.seep.comm;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class InvalidEndPointException extends SeepException {

	private static final long serialVersionUID = 1L;

	public InvalidEndPointException(){
		super();
	}
	
	public InvalidEndPointException(String msg){
		super(msg);
	}
}
