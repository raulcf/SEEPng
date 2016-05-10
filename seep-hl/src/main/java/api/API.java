package api;

import java.util.Map;

import api.io.IO;
import api.lviews.LogicalView;
import api.objects.Locatable;
import api.placing.DataLayout;
import api.topology.Cluster;
import ir.Traceable;

/**
 * Contains the API that developers use to write their applications.
 * It also contains a cluster implementation that gives access to characteristics
 * of the cluster where the program is meant to run.
 * @author ra-mit
 *
 */
public interface API extends IO, DataLayout {

	public Map<Integer, Traceable> getTraces();

	public void addCluster(Cluster c);
	public int gridRows();
	public int gridCols();
	
	public <T extends Locatable> LogicalView<T> createLogicalView();
	
}
