package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;

public class RouterFactory {

	final static private Logger LOG = LoggerFactory.getLogger(RouterFactory.class.getName());
	
	public static Router buildRouterFor(List<DownstreamConnection> cons) {
		if(cons.size() < 1){
			throw new InvalidParameterException("Tried to build router with less than 1 connection");
		}
		boolean stateful = cons.get(0).getDownstreamOperator().isStateful();
		Router rs = null;
		List<Integer> opIds = getListOpId(cons); 
		if(stateful){
			LOG.info("Building ConsistentHashingRoutingState Router");
			rs = new ConsistentHashingRoutingState(opIds);
		}
		else{
			LOG.info("Building RoundRobinRoutingState Router");
			rs = new RoundRobinRoutingState(opIds);
		}
		return rs;
	}
	
	public static Router buildRouterFor(List<Integer> opIds, boolean stateful) {
		if(opIds.size() < 1){
			throw new InvalidParameterException("Tried to build router with less than 1 connection");
		}
		Router rs = null;
		if(stateful){
			LOG.info("Building ConsistentHashingRoutingState Router");
			rs = new ConsistentHashingRoutingState(opIds);
		}
		else{
			LOG.info("Building RoundRobinRoutingState Router");
			rs = new RoundRobinRoutingState(opIds);
		}
		return rs;
	}
	
	private static List<Integer> getListOpId(List<DownstreamConnection> cons){
		List<Integer> toReturn = new ArrayList<>();
		for(DownstreamConnection dc : cons){
			toReturn.add(dc.getDownstreamOperator().getOperatorId());
		}
		return toReturn;
	}
	
}

