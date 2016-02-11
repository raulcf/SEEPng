package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.List;
import java.util.Set;

import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seep.scheduler.Stage;

public interface LoadBalancingStrategy {

	public List<CommandToNode> assignWorkToWorkers(Stage stage, Set<Connection> conns);
}
