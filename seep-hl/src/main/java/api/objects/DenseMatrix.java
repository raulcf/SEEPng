package api.objects;

import java.util.List;

import api.topology.GridPosition;

public class DenseMatrix implements Locatable {

	final static public String N = "matrix.dim.n";
	final static public String M = "matrix.dim.m";
	
	private GridPosition gridPosition;
	
	public DenseMatrix addMatrix(DenseMatrix m) {
		
		return this;
	}
	
	public DenseMatrix multiply(DenseMatrix m) {
		// TODO:
		return this;
	}
	
	public List<Integer> svd() {
		// TODO:
		return null;
	}
	
	/**
	 * Implementation of Locatable
	 */
	
	@Override
	public GridPosition getPositionInTopology() {
		return gridPosition;
	}

	@Override
	public int rowIndex() {
		return gridPosition.getRowIdx();
	}

	@Override
	public int colIndex() {
		return gridPosition.getColIdx();
	}

	@Override
	public void moveTo(int i, int j) {
		// TODO change gridPosition
		
	}

}
