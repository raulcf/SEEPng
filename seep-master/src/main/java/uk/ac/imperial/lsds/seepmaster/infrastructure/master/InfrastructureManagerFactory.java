package uk.ac.imperial.lsds.seepmaster.infrastructure.master;

import uk.ac.imperial.lsds.seep.infrastructure.InfrastructureManager;
import uk.ac.imperial.lsds.seepcontrib.yarn.infrastructure.YarnClusterManager;

public class InfrastructureManagerFactory {

	public static String nameInfrastructureManagerWithType(int infType){
		String name = null;
		if(infType == InfrastructureType.PHYSICAL_CLUSTER.ofType()) {
			name = InfrastructureType.PHYSICAL_CLUSTER.name();
		}
		if (infType == InfrastructureType.YARN_CLUSTER.ofType()) {
		    name = InfrastructureType.YARN_CLUSTER.name();
		}
		return name;
	}
	
	public static InfrastructureManager createInfrastructureManager(int infType){
		if(infType == InfrastructureType.PHYSICAL_CLUSTER.ofType()) {
			return new PhysicalClusterManager();
		}
		else if(infType == InfrastructureType.YARN_CLUSTER.ofType()){
		    return new YarnClusterManager();
		}
		return null;
	}
	
}
