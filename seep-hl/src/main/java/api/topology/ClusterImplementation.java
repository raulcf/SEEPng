package api.topology;

public class ClusterImplementation implements Cluster {

	private int numberOfNodesInCluster;
	
	@Override
	public void setNumberNodes(int numNodes) {
		this.numberOfNodesInCluster = numNodes;
	}

	@Override
	public int gridRows() {
		return (int)Math.sqrt(numberOfNodesInCluster);
	}

	@Override
	public int gridCols() {
		return (int)Math.sqrt(numberOfNodesInCluster);
	}

}
