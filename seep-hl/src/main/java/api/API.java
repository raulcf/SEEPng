package api;

import api.io.IO;
import api.lviews.LogicalView;
import api.objects.Locatable;
import api.placing.DataLayout;
import api.topology.Cluster;
import api.topology.ClusterImplementation;

/**
 * Contains the API that developers use to write their applications.
 * It also contains a cluster implementation that gives access to characteristics
 * of the cluster where the program is meant to run.
 * @author ra-mit
 *
 */
public interface API extends IO, DataLayout {

	final Cluster c = new ClusterImplementation();
	
	public <T extends Locatable> LogicalView<T> createLogicalView();
	
}
