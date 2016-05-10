package api.objects;

import api.topology.GridPosition;
import ir.Traceable;

public interface Locatable extends Traceable {

	public GridPosition getPositionInTopology();
	public int rowIndex();
	public int colIndex();
	
	public void moveTo(int i, int j);
	
}
