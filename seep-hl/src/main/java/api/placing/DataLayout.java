package api.placing;

import api.lviews.LogicalView;
import api.objects.Locatable;
import api.topology.Cluster;

public interface DataLayout {

	public <T extends Locatable> boolean blockDistribution(LogicalView<T> lv, Cluster c);
	public <T extends Locatable> boolean cyclicDistribution(LogicalView<T> lv, Cluster c);
	public <T extends Locatable> boolean blockCyclicDistribution(LogicalView<T> lv, Cluster c);
	public <T extends Locatable> boolean shuffleDistribution(LogicalView<T> lv, Cluster c);
	
	public <T extends Locatable> boolean partitionAndBlockDistribution(LogicalView<T> lv, Cluster c, Partitioner p);
	public <T extends Locatable> boolean partitionAndCyclicDistribution(LogicalView<T> lv, Cluster c, Partitioner p);
	public <T extends Locatable> boolean partitionAndBlockCyclicDistribution(LogicalView<T> lv, Cluster c, Partitioner p);
	public <T extends Locatable> boolean partitionAndShuffleDistribution(LogicalView<T> lv, Cluster c, Partitioner p);
	
}
