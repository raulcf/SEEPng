package uk.ac.imperial.lsds.seepmaster.query;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;

public interface QueryManager {

	public boolean loadQueryFromParameter(SeepLogicalQuery slq);
	public boolean loadQueryFromFile(String pathToJar, String definitionClass, String[] queryArgs);
	public boolean deployQueryToNodes();
	public boolean startQuery();
	public boolean stopQuery();
	
}
