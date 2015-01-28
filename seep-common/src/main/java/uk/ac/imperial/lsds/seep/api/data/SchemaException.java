package uk.ac.imperial.lsds.seep.api.data;

import uk.ac.imperial.lsds.seep.errors.SeepException;

public class SchemaException extends SeepException {

	private static final long serialVersionUID = 1L;

	public SchemaException(String str){
		super(str);
	}
	
}
