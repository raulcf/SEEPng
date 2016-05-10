package api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import api.lviews.LogicalView;
import api.lviews.Mock;
import api.objects.Locatable;
import api.placing.Partitioner;
import api.topology.Cluster;
import ir.TraceSeed;
import ir.Traceable;

public class APIImplementation implements API {
	
	private Cluster c;
	
	private int id = 0;
	
	private Map<Integer, Traceable> traces = new HashMap<>();
	
	@Override
	public void addCluster(Cluster c) {
		this.c = c;
	}
	
	@Override
	public int gridRows() {
		return this.c.gridRows();
	}

	@Override
	public int gridCols() {
		return this.c.gridCols();
	}
	
	@Override
	public <T extends Locatable> LogicalView<T> createLogicalView() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<Integer, Traceable> getTraces() {
		return traces;
	}
	
	/**
	 * Implementation of IO interface 
	 */
	
	@Override
	public <T extends Locatable> LogicalView<T> readFromPath(String filename) {
		// Creates a new task with an id
		int seedId = id++;
		TraceSeed d = new TraceSeed(seedId);
		d.setName("readFromPath: " + filename);
		// The task has 1 output
		Mock m = Mock.makeMockLogicalView(id++);
		Set<T> objs = m.getObjects();
		int oId = m.getId();
		// The output is added to the task
		for(T obj : objs) {
			d.addOutput(obj);
		}
		// Store the created object
		traces.put(seedId, d);
		
		// return so that execution of the program can continue
		return m;
	}

	@Override
	public <T extends Locatable> void writeToPath(LogicalView<T> lv, String filename) {
		int seedId = id++;
		TraceSeed d = new TraceSeed(seedId);
		d.setName("writeToPath: " + filename);
		
		//int iId = lv.getId();
		for(T obj : lv.getObjects()) {
			d.addInput(obj);
		}
		traces.put(seedId, d);
	}
	
	/**
	 * Implementation of DataLayout interface
	 */

	@Override
	public <T extends Locatable> boolean blockDistribution(LogicalView<T> lv) {
		
		TraceSeed st = new TraceSeed(id++); // this will be translated into more specific actions, such as move there, or come here
		st.setName("blockDistribution ");
		// input objects
		for(T obj : lv.getObjects()) {
			st.addInput(obj);
		}
		
		//output objects (may be the same, identity, but may be in a different position)
		for(T obj : lv.getObjects()) {
			st.addOutput(obj);
		}
		
		return false;
	}

	@Override
	public <T extends Locatable> boolean cyclicDistribution(LogicalView<T> lv) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean blockCyclicDistribution(LogicalView<T> lv) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean shuffleDistribution(LogicalView<T> lv) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndBlockDistribution(LogicalView<T> lv, Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndCyclicDistribution(LogicalView<T> lv, Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndBlockCyclicDistribution(LogicalView<T> lv,	Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T extends Locatable> boolean partitionAndShuffleDistribution(LogicalView<T> lv, Partitioner p) {
		// TODO Auto-generated method stub
		return false;
	}

}
