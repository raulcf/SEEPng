package uk.ac.imperial.lsds.seepmaster.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.SeepPhysicalOperator;
import uk.ac.imperial.lsds.seep.api.SeepPhysicalQuery;
import uk.ac.imperial.lsds.seep.comm.Comm;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.comm.protocol.MasterWorkerCommand;
import uk.ac.imperial.lsds.seep.comm.protocol.ProtocolCommandFactory;
import uk.ac.imperial.lsds.seep.comm.serialization.KryoFactory;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepmaster.LifecycleManager;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.ExecutionUnit;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

import com.esotericsoftware.kryo.Kryo;

public class MaterializedQueryManager implements QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(MaterializedQueryManager.class);
	
	private static MaterializedQueryManager qm;
	private LifecycleManager lifeManager;
	private String pathToQueryJar;
	private SeepLogicalQuery slq;
	private SeepPhysicalQuery originalQuery;
	private SeepPhysicalQuery runtimeQuery;
	private int executionUnitsRequiredToStart;
	private InfrastructureManager inf;
	private Map<Integer, EndPoint> opToEndpointMapping;
	private final Comm comm;
	private final Kryo k;
	
	// convenience method for testing
	public static MaterializedQueryManager buildTestMaterializedQueryManager(SeepLogicalQuery lsq, InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm){
		return new MaterializedQueryManager(lsq, inf, mapOpToEndPoint, comm);
	}
	
	// convenience method for testing
	public SeepPhysicalQuery createOriginalPhysicalQueryFrom(SeepLogicalQuery lsq) {
		this.slq = lsq;
		return this.createOriginalPhysicalQuery();
	}
	
	private MaterializedQueryManager(SeepLogicalQuery lsq, InfrastructureManager inf, Map<Integer, EndPoint> opToEndpointMapping, Comm comm){
		this.slq = lsq;
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(lsq);
		this.inf = inf;
		this.opToEndpointMapping = opToEndpointMapping;
		this.comm = comm;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
	}
	
	private MaterializedQueryManager(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm, LifecycleManager lifeManager){
		this.inf = inf;
		this.opToEndpointMapping = mapOpToEndPoint;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
	}
	
	public static MaterializedQueryManager getInstance(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm, LifecycleManager lifeManager){
		if(qm == null){
			return new MaterializedQueryManager(inf, mapOpToEndPoint, comm, lifeManager);
		}
		else{
			return qm;
		}
	}
	
	private boolean canStartExecution(){
		return inf.executionUnitsAvailable() >= executionUnitsRequiredToStart;
	}
	
	@Override
	public boolean loadQueryFromParameter(SeepLogicalQuery slq, String pathToQueryJar) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.slq = slq;
		this.pathToQueryJar = pathToQueryJar;
		LOG.debug("Logical query loaded: {}", slq.toString());
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(slq);
		LOG.info("New query requires: {} units to start execution", this.executionUnitsRequiredToStart);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean loadQueryFromFile(String pathToQueryJar, String definitionClass, String[] queryArgs) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.pathToQueryJar = pathToQueryJar;
		// FIXME: eliminate hardcoded name
		// get logical query 
		this.slq = Utils.executeComposeFromQuery(pathToQueryJar, definitionClass, queryArgs, "compose");
		LOG.debug("Logical query loaded: {}", slq.toString());
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(slq);
		LOG.info("New query requires: {} units to start execution", this.executionUnitsRequiredToStart);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
	@Override
	public boolean deployQueryToNodes() {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// Check whether there are sufficient execution units to deploy query
		if(!canStartExecution()){
			LOG.warn("Cannot deploy query, not enough nodes. Required: {}, available: {}"
					, executionUnitsRequiredToStart, inf.executionUnitsAvailable());
			return false;
		}
		LOG.info("Building physicalQuery from logicalQuery...");
		originalQuery = createOriginalPhysicalQuery();
		LOG.debug("Building physicalQuery from logicalQuery...OK {}", originalQuery.toString());
		Set<Integer> involvedEUId = originalQuery.getIdOfEUInvolved();
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		sendQueryInformationToNodes(connections);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_DEPLOYED);
		return true;
	}
	
	@Override
	public boolean startQuery(){
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_RUNNING);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// TODO: take a look at the following two lines. Stateless is good to keep everything lean. Yet consider caching
		Set<Integer> involvedEUId = originalQuery.getIdOfEUInvolved();
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		// Send start query command
		MasterWorkerCommand start = ProtocolCommandFactory.buildStartQueryCommand();
		comm.send_object_sync(start, connections, k);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_RUNNING);
		return true;
	}
	
	@Override
	public boolean stopQuery(){
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_STOPPED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		// TODO: take a look at the following two lines. Stateless is good to keep everything lean. Yet consider caching
		Set<Integer> involvedEUId = originalQuery.getIdOfEUInvolved();
		Set<Connection> connections = inf.getConnectionsTo(involvedEUId);
		
		// Send start query command
		MasterWorkerCommand stop = ProtocolCommandFactory.buildStopQueryCommand();
		comm.send_object_sync(stop, connections, k);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_STOPPED);
		return true;
	}
	
	private SeepPhysicalQuery createOriginalPhysicalQuery(){
		Set<SeepPhysicalOperator> physicalOperators = new HashSet<>();
		
		// use pre-defined description if exists
		if(this.opToEndpointMapping != null){
			for(Entry<Integer, EndPoint> e : opToEndpointMapping.entrySet()){
				// TODO: implement manual mapping from the description
			}
		}
		// otherwise map to random workers
		else{
			this.opToEndpointMapping = new HashMap<>();
			
			for(Operator lso : slq.getAllOperators()){
				ExecutionUnit eu = inf.getExecutionUnit();
				EndPoint ep = eu.getEndPoint();
				SeepPhysicalOperator po = SeepPhysicalOperator.createPhysicalOperatorFromLogicalOperatorAndEndPoint(lso, ep);
				int pOpId = po.getOperatorId();
				LOG.debug("LogicalOperator: {} will run on: {} -> ({})", pOpId, po.getWrappingEndPoint().getId(), po.getWrappingEndPoint().getIp().toString());
				opToEndpointMapping.put(pOpId, po.getWrappingEndPoint());
				physicalOperators.add(po);
			}
		}
		SeepPhysicalQuery psq = SeepPhysicalQuery.buildPhysicalQueryFrom(physicalOperators, slq);
		return psq;
	}
	
	private int computeRequiredExecutionUnits(SeepLogicalQuery lsq){
		return lsq.getAllOperators().size();
	}
	
	private void sendQueryInformationToNodes(Set<Connection> connections){
		// Send data file to nodes
		byte[] queryFile = Utils.readDataFromFile(pathToQueryJar);
		LOG.info("Sending query file of size: {} bytes", queryFile.length);
		MasterWorkerCommand code = ProtocolCommandFactory.buildCodeCommand(queryFile);
		comm.send_object_sync(code, connections, k);
		LOG.info("Sending query file...DONE!");
		LOG.info("Sending Query Deploy Command");
		// Send physical query to all nodes
		MasterWorkerCommand queryDeploy = ProtocolCommandFactory.buildQueryDeployCommand(originalQuery);
		comm.send_object_sync(queryDeploy, connections, k);
		LOG.info("Sending Query Deploy Command...DONE!");
	}
	
}