package api.lviews;

import java.util.List;
import java.util.Set;

import api.objects.Locatable;
import ir.Traceable;

public class Mock<T extends Locatable> implements LogicalView<T>{

	// Traceable attributes
	private int id;
	private String name;
	private List<Traceable> inputs;
	private List<Traceable> outputs;
	
	public Mock (int id) {
		this.id = id;
	}
	
	public static Mock makeMockLogicalView(int id) {
		return new Mock(id);
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
		// TODO Auto-generated method stub
		return null;
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
