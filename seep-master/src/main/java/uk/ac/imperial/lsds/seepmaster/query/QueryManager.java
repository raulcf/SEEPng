package uk.ac.imperial.lsds.seepmaster.query;

import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public interface QueryManager {

	public boolean loadQueryFromParameter(SeepLogicalQuery slq, String pathToQueryJar,  String definitionClass, String[] queryArgs, String composeMethod);
	public boolean loadQueryFromFile(String pathToQueryJar, String definitionClass, String[] queryArgs, String composeMethod);
	public boolean deployQueryToNodes();
	public boolean startQuery();
	public boolean stopQuery();
	
}
