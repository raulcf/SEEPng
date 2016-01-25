package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnitType;

public class PhysicalClusterManager implements InfrastructureManager {
	
	final private Logger LOG = LoggerFactory.getLogger(PhysicalClusterManager.class);
	
	public final ExecutionUnitType executionUnitType = ExecutionUnitType.PHYSICAL_NODE;
	private Deque<ExecutionUnit> availablePhysicalNodes;
	private Deque<ExecutionUnit> usedPhysicalNodes;
	private Map<Integer, Connection> connectionsToPhysicalNodes;

	public PhysicalClusterManager(){
		this.availablePhysicalNodes = new ArrayDeque<>();
		this.usedPhysicalNodes = new ArrayDeque<>();
		this.connectionsToPhysicalNodes = new HashMap<>();
	}
	
	@Override
	public ExecutionUnit buildExecutionUnit(InetAddress ip, int port, int dataPort, int controlPort) {
		return new PhysicalNode(ip, port, dataPort, controlPort);
	}
	
	@Override
	public void addExecutionUnit(ExecutionUnit eu) {
		availablePhysicalNodes.push(eu);
		connectionsToPhysicalNodes.put(eu.getId(), new Connection(eu.getEndPoint().extractMasterControlEndPoint()));
	}
	
	@Override
	public ExecutionUnit getExecutionUnit(){
		if(availablePhysicalNodes.size() > 0){
			LOG.debug("Returning 1 executionUnit, remaining: {}", availablePhysicalNodes.size()-1);
			ExecutionUnit toReturn = availablePhysicalNodes.pop();
			usedPhysicalNodes.push(toReturn);
			return toReturn;
		}
		else{
			LOG.error("No available executionUnits !!!");
			return null;
		}
	}

	@Override
	public boolean removeExecutionUnit(int id) {
		for(ExecutionUnit eu : usedPhysicalNodes){
			if(eu.getId() == id){
				boolean success = usedPhysicalNodes.remove(eu);
				if(success){
					LOG.info("ExecutionUnit id: {} was removed from usedPhysicalNodes", id);
				}
			}
		}
		for(ExecutionUnit eu : availablePhysicalNodes){
			if(eu.getId() == id){
				boolean success = availablePhysicalNodes.remove(eu);
				if(success){
					LOG.info("ExecutionUnit id: {} was removed from availablePhysicalNodes", id);
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public int executionUnitsAvailable() {
		return availablePhysicalNodes.size();
	}
	
	@Override
	public Collection<ExecutionUnit> executionUnitsInUse() {
		return usedPhysicalNodes;
	}

	@Override
	public void claimExecutionUnits(int numExecutionUnits) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void decommisionExecutionUnits(int numExecutionUnits) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void decommisionExecutionUnit(ExecutionUnit eu) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<Connection> getConnectionsTo(Set<Integer> executionUnitIds) {
		Set<Connection> cs = new HashSet<>();
		for(Integer id : executionUnitIds) {
			// TODO: check that the conn actually exists
			cs.add(connectionsToPhysicalNodes.get(id));
		}
		return cs;
	}

	@Override
	public Connection getConnectionTo(int executionUnitId) {
		return connectionsToPhysicalNodes.get(executionUnitId);
	}

}
