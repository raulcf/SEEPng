package api;

import api.lviews.LogicalView;
import api.objects.Locatable;
import api.placing.Partitioner;
import api.topology.Cluster;

public class APIImplementation implements API {

	@Override
	public <T extends Locatable> LogicalView<T> createLogicalView() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public <T extends Locatable> LogicalView<T> readFromPath(String filename) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends Locatable> void writeToPath(LogicalView<T> lv, String filename) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T extends Locatable> boolean blockDistribution(LogicalView<T> lv, Cluster c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean cyclicDistribution(LogicalView<T> lv, Cluster c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean blockCyclicDistribution(LogicalView<T> lv, Cluster c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean shuffleDistribution(LogicalView<T> lv, Cluster c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndBlockDistribution(LogicalView<T> lv, Cluster c, Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndCyclicDistribution(LogicalView<T> lv, Cluster c, Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndBlockCyclicDistribution(LogicalView<T> lv, Cluster c,
			Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndShuffleDistribution(LogicalView<T> lv, Cluster c, Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

}
