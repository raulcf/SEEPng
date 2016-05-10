package api;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import api.lviews.LogicalView;
import api.lviews.Mock;
import api.objects.Locatable;
import api.placing.Partitioner;
import api.topology.Cluster;
import ir.Dummy;

public class APIImplementation implements API {
	
	private int id = 0;
	
	private Map<Integer, Dummy> dummies = new HashMap<>();
	
	@Override
	public <T extends Locatable> LogicalView<T> createLogicalView() {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
	 * Implementation of IO interface 
	 */
	
	@Override
	public <T extends Locatable> LogicalView<T> readFromPath(String filename) {
		// Creates a new task with an id
		Dummy d = new Dummy(id++);
		d.setName("readFromPath: " + filename);
		// The task has 1 output
		Mock m = Mock.makeMockLogicalView(id++);
		int oId = m.getId();
		// The output is added to the task
		d.addOutput(oId);
		// Store the created object
		dummies.put(id, d);
		
		// return so that execution of the program can continue
		return m;
	}

	@Override
	public <T extends Locatable> void writeToPath(LogicalView<T> lv, String filename) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * Implementation of DataLayout interface
	 */

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
