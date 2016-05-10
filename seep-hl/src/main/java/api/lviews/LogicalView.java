package api.lviews;

import java.util.Set;

import api.objects.Locatable;
import ir.Traceable;

public interface LogicalView<T extends Locatable> extends Traceable {
	
	public int getId();
	
	public int getMetadata(String key);
	
	public Set<T> getObjects();
	
	public T position(int i, int j);
	
	public void assign(Locatable data, int i, int j);
	
}
