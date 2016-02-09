package uk.ac.imperial.lsds.seep.lapi;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public interface LQueryComposer {

	public static LQueryBuilder LqueryAPI = new LQueryBuilder();
	
	public SeepLogicalQuery compose();
	
}
