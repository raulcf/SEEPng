package api.placing;

import api.lviews.LogicalView;
import api.objects.Locatable;

public interface DataLayout {

	public <T extends Locatable> boolean blockDistribution(LogicalView<T> lv);
	public <T extends Locatable> boolean cyclicDistribution(LogicalView<T> lv);
	public <T extends Locatable> boolean blockCyclicDistribution(LogicalView<T> lv);
	public <T extends Locatable> boolean shuffleDistribution(LogicalView<T> lv);
	
	public <T extends Locatable> boolean partitionAndBlockDistribution(LogicalView<T> lv, Partitioner p);
	public <T extends Locatable> boolean partitionAndCyclicDistribution(LogicalView<T> lv, Partitioner p);
	public <T extends Locatable> boolean partitionAndBlockCyclicDistribution(LogicalView<T> lv, Partitioner p);
	public <T extends Locatable> boolean partitionAndShuffleDistribution(LogicalView<T> lv, Partitioner p);
	
}
