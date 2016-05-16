package api.topology;

public class GridPosition {

	private int i;
	private int j;
	
	public GridPosition(int i, int j) {
		this.i = i;
		this.j = j;
	}
	
	public int getRowIdx() {
		return i;
	}
	
	public int getColIdx() {
		return j;
	}
	
	public String toString() {
		return "GRID["+i+"]["+j+"]";
	}
	
}
