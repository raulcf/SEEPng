package api.lviews;

import java.util.HashSet;
import java.util.Set;

import api.objects.DenseMatrix;
import api.objects.Locatable;
import api.topology.Cluster;

public class Mock<T extends Locatable> implements LogicalView<T> {

	// Traceable attributes
	private int id;
	
	// Objects
	private Set<T> objects;
	
	public Mock (int id, Cluster c) {
		this.id = id;
		this.objects = new HashSet<>();
		for (int i = 0; i < c.getNumberNodes(); i++) {
			objects.add((T) new DenseMatrix());
		}
	}
	
	public static Mock makeMockLogicalView(int id, Cluster c) {
		return new Mock(id, c);
	}
	
	@Override
	public int getId() {
		return id;
	}
	
	/**
	 * Implementation of LogicalView interface
	 */

	@Override
	public int getMetadata(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Set<T> getObjects() {
		return objects;
	}

	@Override
	public T position(int i, int j) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void assign(Locatable data, int i, int j) {
		// TODO Auto-generated method stub
		
	}

}
