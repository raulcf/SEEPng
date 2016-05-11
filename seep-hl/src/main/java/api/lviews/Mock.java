package api.lviews;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import api.objects.DenseMatrix;
import api.objects.Locatable;
import api.topology.Cluster;
import ir.IdGen;

public class Mock<T extends Locatable> implements LogicalView<T> {

	// Traceable attributes
	private int id;
	
	// Objects
	private Set<T> objects;
	
	private T[][] grid;
	
	public Mock (int id, Cluster c, IdGen idGen) {
		this.id = id;
		this.objects = new HashSet<>();
		for (int i = 0; i < c.getNumberNodes(); i++) {
			objects.add((T) new DenseMatrix(idGen.id(), "denseM_init"));
		}
		int rows = c.gridRows();
		int cols = c.gridCols();
		Iterator<T> it = objects.iterator();
		grid = (T[][]) new DenseMatrix[rows][cols];
		for(int i = 0; i < rows; i++) {
			for(int j = 0; j < cols; j++) {
				grid[i][j] = it.next();
			}
		}
	}
	
	public static Mock makeMockLogicalView(int id, Cluster c, IdGen idGen) {
		return new Mock(id, c, idGen);
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
		return grid[i][j];
	}

	@Override
	public void assign(Locatable data, int i, int j) {
		// TODO Auto-generated method stub
		
	}

}
