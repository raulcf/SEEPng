package uk.ac.imperial.lsds.seepmaster.query;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

public class GenericQueryManager implements QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(GenericQueryManager.class);
	
	// Singleton instance made available to client (Master)
	private static GenericQueryManager gqm;
	// The actual QueryManager backend
	private QueryManager qm;
	
	private InfrastructureManager inf;
	private Map<Integer, EndPoint> opToEndpointMapping;
	private Comm comm;
	private LifecycleManager lifeManager;
	
	public static GenericQueryManager getInstance(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm, LifecycleManager lifeManager){
		if(gqm == null){
			return new GenericQueryManager(inf, mapOpToEndPoint, comm, lifeManager);
		}
		else{
			return gqm;
		}
	}
	
	private GenericQueryManager(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm, LifecycleManager lifeManager){
		this.inf = inf;
		this.opToEndpointMapping = mapOpToEndPoint;
		this.comm = comm;
		this.lifeManager = lifeManager;
	}
	
	@Override
	public boolean loadQueryFromParameter(SeepLogicalQuery slq) {
		// Check whether the action is valid, but GenericQueryManager does not change Lifecycle
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		LOG.debug("Logical query loaded: {}", slq.toString());
		// This is the only operation fully handled by QueryManager
		// After checking queryExecutionMode it creates an appropriate queryManager and 
		// delegates all operations to that one
		QueryExecutionMode qem = slq.getQueryExecutionMode();
		qm = this.getQueryManagerForExecutionMode(qem);
		boolean success = qm.loadQueryFromParameter(slq);
		return success;
	}
	
	@Override
	public boolean loadQueryFromFile(String pathToJar, String definitionClass, String[] queryArgs) {
		// Check whether the action is valid, but GenericQueryManager does not change Lifecycle
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// FIXME: eliminate hardcoded name
		// get logical query
		SeepLogicalQuery lsq = Utils.executeComposeFromQuery(pathToJar, definitionClass, queryArgs, "compose");
		return this.loadQueryFromParameter(lsq);
	}
	
	private QueryManager getQueryManagerForExecutionMode(QueryExecutionMode qem) {
		QueryManager qm = null;
		switch (qem){
		case ALL_MATERIALIZED:
			qm = MaterializedQueryManager.getInstance(inf, opToEndpointMapping, comm, lifeManager);
			break;
		case ALL_SCHEDULED:
			qm = ScheduledQueryManager.getInstance(inf, opToEndpointMapping, comm, lifeManager);
			break;
		case AUTOMATIC_HYBRID:
			
			break;
		default:
			
		}
		return qm;
	}

	@Override
	public boolean deployQueryToNodes() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean startQuery() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean stopQuery() {
		// TODO Auto-generated method stub
		return false;
	}

}
