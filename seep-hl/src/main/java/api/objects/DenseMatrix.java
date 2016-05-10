package api.objects;

import java.util.List;

import api.topology.GridPosition;
import ir.TraceSeed;
import ir.Traceable;

public class DenseMatrix implements Locatable {

	final static public String N = "matrix.dim.n";
	final static public String M = "matrix.dim.m";
	
	private GridPosition gridPosition;
	
	public DenseMatrix addMatrix(DenseMatrix m) {
		// Trace action
		
		TraceSeed ts = new TraceSeed(100); // this op
		ts.setName("addMatrix");
		this.addOutput(ts); // to chain with the existing object
		ts.addInput(this);
		
		// actual operation would occur here
		// FIXME: instead of this, we'd pass the new matrix
		ts.addOutput(this);
		
		// perform operation
		return this;
	}
	
	public DenseMatrix multiply(DenseMatrix m) {
		// Trace action
		
		TraceSeed ts = new TraceSeed(100); // this op
		ts.setName("multiply");
		this.addOutput(ts); // to chain with the existing object
		ts.addInput(this);
		
		// actual operation would occur here
		// FIXME: instead of this, we'd pass the new matrix
		ts.addOutput(this);
		
		// perform operation
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
	
	/**
	 * Implementation of Traceable interface (extended by Locatable)
	 */

	@Override
	public int getId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setName(String name) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addInput(Traceable t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addOutput(Traceable t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void isInputOf(Traceable t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void isOutputOf(Traceable t) {
		// TODO Auto-generated method stub
		
	}

}
