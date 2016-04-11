package api;

import api.io.IO;
import api.lviews.LogicalView;
import api.objects.Locatable;
import api.placing.DataLayout;
import api.topology.Cluster;
import api.topology.ClusterImplementation;

public interface API extends IO, DataLayout {

	final public Cluster c = new ClusterImplementation();
	
	public <T extends Locatable> LogicalView<T> createLogicalView();
}
