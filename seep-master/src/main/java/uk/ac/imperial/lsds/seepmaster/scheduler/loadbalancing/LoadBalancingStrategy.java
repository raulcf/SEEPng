package uk.ac.imperial.lsds.seepmaster.scheduler.loadbalancing;

import java.util.List;

import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seepmaster.infrastructure.master.InfrastructureManager;
import uk.ac.imperial.lsds.seepmaster.scheduler.ClusterDatasetRegistry;
import uk.ac.imperial.lsds.seepmaster.scheduler.CommandToNode;

public interface LoadBalancingStrategy {

	public List<CommandToNode> assignWorkToWorkers(Stage stage, InfrastructureManager inf, ClusterDatasetRegistry cdr);
}
