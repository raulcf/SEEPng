package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.List;

import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;

public interface LoadBalancingStrategy {

	public List<CommandToNode> assignWorkToWorkers(Stage stage, InfrastructureManager inf, ClusterDatasetRegistry cdr);
}
