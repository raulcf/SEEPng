package uk.ac.imperial.lsds.seepmaster.query;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.PhysicalOperator;
import uk.ac.imperial.lsds.seep.api.PhysicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.SeepQueryPhysicalOperator;
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

public class QueryManager {

	final private Logger LOG = LoggerFactory.getLogger(QueryManager.class);
	
	private static QueryManager qm;
	private LifecycleManager lifeManager;
	private String pathToQuery;
	private LogicalSeepQuery lsq;
	private PhysicalSeepQuery originalQuery;
	private PhysicalSeepQuery runtimeQuery;
	private int executionUnitsRequiredToStart;
	private InfrastructureManager inf;
	private Map<Integer, EndPoint> opToEndpointMapping;
	private final Comm comm;
	private final Kryo k;
	
	public PhysicalSeepQuery getOriginalPhysicalQuery(){
		return originalQuery;
	}
	
	public PhysicalSeepQuery getRuntimePhysicalQuery(){
		return runtimeQuery;
	}
	
	public QueryManager(LogicalSeepQuery lsq, InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm){
		this.lsq = lsq;
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(lsq);
		this.inf = inf;
		this.opToEndpointMapping = mapOpToEndPoint;
		this.comm = comm;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
	}
	
	private QueryManager(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm, LifecycleManager lifeManager){
		this.inf = inf;
		this.opToEndpointMapping = mapOpToEndPoint;
		this.comm = comm;
		this.lifeManager = lifeManager;
		this.k = KryoFactory.buildKryoForMasterWorkerProtocol();
	}
	
	public static QueryManager getInstance(InfrastructureManager inf, Map<Integer, EndPoint> mapOpToEndPoint, Comm comm, LifecycleManager lifeManager){
		if(qm == null){
			return new QueryManager(inf, mapOpToEndPoint, comm, lifeManager);
		}
		else{
			return qm;
		}
	}
	
	private boolean canStartExecution(){
		return inf.executionUnitsAvailable() >= executionUnitsRequiredToStart;
	}
	
	public boolean loadQueryFromFile(String pathToJar, String definitionClass, String[] queryArgs) {
		boolean allowed = lifeManager.canTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		if(!allowed){
			LOG.error("Attempt to violate application lifecycle");
			return false;
		}
		this.pathToQuery = pathToJar;
		// get logical query
		this.lsq = executeComposeFromQuery(pathToJar, definitionClass, queryArgs);
		LOG.debug("Logical query loaded: {}", lsq.toString());
		this.executionUnitsRequiredToStart = this.computeRequiredExecutionUnits(lsq);
		LOG.info("New query requires: {} units to start execution", this.executionUnitsRequiredToStart);
		lifeManager.tryTransitTo(LifecycleManager.AppStatus.QUERY_SUBMITTED);
		return true;
	}
	
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
	
	public PhysicalSeepQuery createOriginalPhysicalQueryFrom(LogicalSeepQuery lsq){
		this.lsq = lsq;
		return this.createOriginalPhysicalQuery();
	}
	
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
	
	private PhysicalSeepQuery createOriginalPhysicalQuery(){
		Set<SeepQueryPhysicalOperator> physicalOperators = new HashSet<>();
		
		// use pre-defined description if exists
		if(this.opToEndpointMapping != null){
			for(Entry<Integer, EndPoint> e : opToEndpointMapping.entrySet()){
				// TODO: implement manual mapping from the description
			}
		}
		// otherwise map to random workers
		else{
			this.opToEndpointMapping = new HashMap<>();
			
			for(Operator lso : lsq.getAllOperators()){
				ExecutionUnit eu = inf.getExecutionUnit();
				EndPoint ep = eu.getEndPoint();
				SeepQueryPhysicalOperator po = SeepQueryPhysicalOperator.createPhysicalOperatorFromLogicalOperatorAndEndPoint(lso, ep);
				int pOpId = po.getOperatorId();
				LOG.debug("LogicalOperator: {} will run on: {} -> ({})", pOpId, po.getWrappingEndPoint().getId(), po.getWrappingEndPoint().getIp().toString());
				opToEndpointMapping.put(pOpId, po.getWrappingEndPoint());
				physicalOperators.add(po);
			}
		}
		PhysicalSeepQuery psq = PhysicalSeepQuery.buildPhysicalQueryFrom(physicalOperators, lsq);
		return psq;
	}
	
	private int computeRequiredExecutionUnits(LogicalSeepQuery lsq){
		return lsq.getAllOperators().size();
	}
	
	private void sendQueryInformationToNodes(Set<Connection> connections){
		// Send data file to nodes
		byte[] queryFile = Utils.readDataFromFile(pathToQuery);
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
	
	private LogicalSeepQuery executeComposeFromQuery(String pathToJar, String definitionClass, String[] queryArgs){
		Class<?> baseI = null;
		Object baseInstance = null;
		Method compose = null;
		LogicalSeepQuery lsq = null;
		File urlPathToQueryDefinition = new File(pathToJar);
		LOG.debug("-> Set path to query definition: {}", urlPathToQueryDefinition.getAbsolutePath());
		URL[] urls = new URL[1];
		try {
			urls[0] = urlPathToQueryDefinition.toURI().toURL();
		}
		catch (MalformedURLException e) {
			e.printStackTrace();
		}
		// First time it is created we pass the urls
		URLClassLoader ucl = new URLClassLoader(urls);
		try {
			baseI = ucl.loadClass(definitionClass);
			// For backwards compatibility, use the default constructor if one with a string array argument is not found
			try {
				baseInstance = baseI.getConstructor(String[].class).newInstance((Object)queryArgs);
			} catch (NoSuchMethodException e) {
				baseInstance = baseI.newInstance();
				if (queryArgs.length > 0) {
					LOG.warn("Query arguments specified but Base class has no constructor taking a String[] argument");
				}
			}
			// FIXME: eliminate hardcoded name
			compose = baseI.getDeclaredMethod("compose", (Class<?>[])null);
			lsq = (LogicalSeepQuery) compose.invoke(baseInstance, (Object[])null);
			ucl.close();
		}
		catch (SecurityException e) {
			e.printStackTrace();
		} 
		catch (NoSuchMethodException e) {
			e.printStackTrace();
		} 
		catch (IllegalArgumentException e) {
			e.printStackTrace();
		} 
		catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		catch (InvocationTargetException e) {
			e.printStackTrace();
		} 
		catch (InstantiationException e) {
			e.printStackTrace();
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		//Finally we return the queryPlan
		return lsq;
	}
}