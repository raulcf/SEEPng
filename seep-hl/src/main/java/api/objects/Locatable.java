package api.objects;

import api.topology.GridPosition;

public interface Locatable {

	public GridPosition getPositionInTopology();
	public int rowIndex();
	public int colIndex();
	
}
