package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;

public class RouterFactory {

	final static private Logger LOG = LoggerFactory.getLogger(RouterFactory.class.getName());
	
	public static Router buildRouterFor(List<DownstreamConnection> cons) {
		boolean stateful = cons.get(0).getDownstreamOperator().isStateful();
		Router rs = null;
		if(stateful){
			LOG.info("Building ConsistentHashingRoutingState Router");
			rs = new ConsistentHashingRoutingState(cons);
		}
		else{
			LOG.info("Building RoundRobinRoutingState Router");
			rs = new RoundRobinRoutingState(cons);
		}
		return rs;
	}
	
	public static Router buildRouterFor(List<DownstreamConnection> cons, boolean stateful) {
		Router rs = null;
		if(stateful){
			LOG.info("Building ConsistentHashingRoutingState Router");
			rs = new ConsistentHashingRoutingState(cons);
		}
		else{
			LOG.info("Building RoundRobinRoutingState Router");
			rs = new RoundRobinRoutingState(cons);
		}
		return rs;
	}
	
}

