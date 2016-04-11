package api.io;

import api.lviews.LogicalView;
import api.objects.Locatable;

public interface IO {

	public <T extends Locatable> LogicalView<T> readFromPath(String filename);
	public <T extends Locatable> void writeToPath(LogicalView<T> lv, String filename);
	
}
