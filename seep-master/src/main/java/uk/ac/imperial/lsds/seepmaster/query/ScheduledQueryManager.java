package uk.ac.imperial.lsds.seepmaster.query;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

import com.esotericsoftware.kryo.Kryo;

public class ScheduledQueryManager implements QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(ScheduledQueryManager.class);
	
	private static ScheduledQueryManager sqm;
	private SeepLogicalQuery slq;
	
	
	private InfrastructureManager inf;
	private Map<Integer, EndPoint> opToEndpointMapping;
	private Comm comm;
	private Kryo k;
	private LifecycleManager lifeManager;
	
	private ScheduledQueryManager(InfrastructureManager inf, Map<Integer, EndPoint> opToEndpointMapping, Comm comm, LifecycleManager lifeManager){
		this.inf = inf;
		this.opToEndpointMapping = opToEndpointMapping;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
	}
	
	public static ScheduledQueryManager getInstance(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm, LifecycleManager lifeManager){
		if(sqm == null){
			return new ScheduledQueryManager(inf, mapOpToEndPoint, comm, lifeManager);
		}
		else{
			return sqm;
		}
	}
	
	@Override
	public boolean loadQueryFromParameter(SeepLogicalQuery slq) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.slq = slq;
		LOG.debug("Logical query loaded: {}", slq.toString());
		
		// TODO: implement this method
		LOG.error("Scheduled engine not implemented...");
		
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean loadQueryFromFile(String pathToJar, String definitionClass, String[] queryArgs) {
		// TODO Auto-generated method stub
		return false;
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
