package uk.ac.imperial.lsds.seep.api;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public interface QueryComposer {
	
	public static QueryBuilder queryAPI = new QueryBuilder();
	
	public SeepLogicalQuery compose();
    
}