package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;

public interface InfrastructureManager {
	
	public ExecutionUnit buildExecutionUnit(InetAddress ip, int controlPort, int dataPort);
	
	public void addExecutionUnit(ExecutionUnit eu);
	public ExecutionUnit getExecutionUnit();
	public boolean removeExecutionUnit(int id);
	public int executionUnitsAvailable();
	public Collection<ExecutionUnit> executionUnitsInUse();
	
	public void claimExecutionUnits(int numExecutionUnits);
	public void decommisionExecutionUnits(int numExecutionUnits);
	public void decommisionExecutionUnit(ExecutionUnit node);
	
	public Set<Connection> getConnectionsTo(Set<Integer> executionUnitIds);
	public Connection getConnectionTo(int executionUnitId);
	
}
