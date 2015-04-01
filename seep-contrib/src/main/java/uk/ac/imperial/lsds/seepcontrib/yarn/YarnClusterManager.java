package uk.ac.imperial.lsds.seepcontrib.yarn;

import java.net.InetAddress;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.infrastructure.ExecutionUnit;
import uk.ac.imperial.lsds.seep.infrastructure.InfrastructureManager;


public class YarnClusterManager implements InfrastructureManager {

	@Override
	public ExecutionUnit buildExecutionUnit(InetAddress ip, int port,
			int dataPort) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addExecutionUnit(ExecutionUnit eu) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ExecutionUnit getExecutionUnit() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean removeExecutionUnit(int id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int executionUnitsAvailable() {
		// TODO Auto-generated method stub
		return 0;
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
	public void decommisionExecutionUnit(ExecutionUnit node) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<Connection> getConnectionsTo(Set<Integer> executionUnitIds) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Connection getConnectionTo(int executionUnitId) {
		// TODO Auto-generated method stub
		return null;
	}

}
