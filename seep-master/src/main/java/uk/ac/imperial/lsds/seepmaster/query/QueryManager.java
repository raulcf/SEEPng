package uk.ac.imperial.lsds.seepmaster.query;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;

public interface QueryManager {

	public boolean loadQueryFromParameter(SeepLogicalQuery slq, String pathToQueryJar);
	public boolean loadQueryFromFile(String pathToQueryJar, String definitionClass, String[] queryArgs, String composeMethod);
	public boolean deployQueryToNodes(String definitionClass, String[] queryArgs, String composeMethod);
	public boolean startQuery();
	public boolean stopQuery();
	
}
