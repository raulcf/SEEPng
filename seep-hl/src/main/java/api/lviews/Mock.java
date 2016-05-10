package api.lviews;

import java.util.Set;

import api.objects.Locatable;

public class Mock<T extends Locatable> implements LogicalView<T>{

	private int id;
	
	public Mock (int id) {
		this.id = id;
	}
	
	public static Mock makeMockLogicalView(int id) {
		return new Mock(id);
	}
	
	/**
	 * Implementation of Traceable interface
	 */
	
	@Override
	public int getId() {
		return id;
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
	public void addInput(int id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addOutput(int id) {
		// TODO Auto-generated method stub
		
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
