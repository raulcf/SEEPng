package api.objects;

import java.util.ArrayList;
import java.util.List;

import api.topology.GridPosition;
import ir.IdGen;
import ir.TraceSeed;
import ir.Traceable;

public class DenseMatrix implements Locatable {

	final static public String N = "matrix.dim.n";
	final static public String M = "matrix.dim.m";
	
	private GridPosition gridPosition;
	
	// Traceable attributes
	private int id;
	private String name;
	private List<Traceable> inputs = new ArrayList<>();
	private List<Traceable> outputs = new ArrayList<>();
	private IdGen idGen;
	
	public DenseMatrix(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public DenseMatrix addMatrix(DenseMatrix m) {
		
		// Trace action
		TraceSeed ts = new TraceSeed(idGen.id()); // this op
		ts.setName("addMatrix");
		ts.addInput(m);
		ts.addInput(this);
		m.addOutput(ts);
		this.addOutput(ts);
		
		// new added matrix
		DenseMatrix dm2 = new DenseMatrix(idGen.id(), "denseM");
		dm2.composeIdGenerator(idGen);
		ts.addOutput(dm2);
		
		// perform operation
		return dm2;
	}
	
	public DenseMatrix multiply(DenseMatrix m) {
		
		// Trace action
		TraceSeed ts = new TraceSeed(idGen.id()); // this op
		ts.setName("multiply");
		ts.addInput(m);
		ts.addInput(this);
		m.addOutput(ts);
		this.addOutput(ts);
		
		// actual operation would occur here
		DenseMatrix dm2 = new DenseMatrix(idGen.id(), "denseM");
		dm2.composeIdGenerator(idGen);
		ts.addOutput(dm2);
		
		// perform operation
		return dm2;
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
	public void composeIdGenerator(IdGen idGen) {
		this.idGen = idGen;
	}

	@Override
	public int getId() {
		return id;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void addInput(Traceable t) {
		inputs.add(t);
	}

	@Override
	public void addOutput(Traceable t) {
		outputs.add(t);
	}

	@Override
	public void isInputOf(Traceable t) {
		t.addInput(this);
	}

	@Override
	public void isOutputOf(Traceable t) {
		t.addOutput(t);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("ID: " + id);
		sb.append(System.lineSeparator());
		sb.append("Name: " + name);
		sb.append(System.lineSeparator());
		
		sb.append("Inputs: " + inputs.size());
		sb.append(System.lineSeparator());
//		for(int i = 0; i < inputs.size(); i++) {
//			sb.append(inputs.get(i));
//			sb.append(System.lineSeparator());
//		}
//		
		sb.append("Outputs: " + outputs.size());
		sb.append(System.lineSeparator());
		for(int i = 0; i < outputs.size(); i++) {
			sb.append(outputs.get(i));
			sb.append(System.lineSeparator());
		}
		
		return sb.toString();
	}

}
