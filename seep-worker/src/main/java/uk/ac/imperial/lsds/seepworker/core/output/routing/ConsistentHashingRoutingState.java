package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seepworker.core.output.OutputBuffer;


public class ConsistentHashingRoutingState implements Router {

	private CRC32 crc32;
	
	private List<DownstreamConnection> cons;
	
	// each downstream id
	private List<Integer> ids;
	// subspace per downstream id
	private List<Integer> subspaceFrontiers;
	
	public ConsistentHashingRoutingState(List<DownstreamConnection> cons){
		this.crc32 = new CRC32();
		this.cons = cons;
		this.ids = new ArrayList<>();
		this.subspaceFrontiers = new ArrayList<>();
		int numSpaces = cons.size();
		// split initial space into the number of cons
		int numSplits = (numSpaces > 1) ? numSpaces - 1 : 1;
		// calculate span of each subrange of the space
		int entireSpace = Integer.MAX_VALUE;
		
		int initialSubspaceSize = entireSpace/numSplits;

		int horizon = Integer.MIN_VALUE;
		for(int i = 0; i < numSpaces; i++){
			// get id to which we'll assign this subspace
			int id = cons.get(i).getDownstreamOperator().getOperatorId();
			int frontier = horizon + initialSubspaceSize;
			ids.add(id);
			subspaceFrontiers.add(frontier);
		}
	}

	@Override
	public OutputBuffer route(Map<Integer, OutputBuffer> obufs, int key) {
		int hashedKey = hashKey(key);
		for(int i = 0; i<subspaceFrontiers.size(); i++){
			int frontier = subspaceFrontiers.get(i);
			if(hashedKey < frontier){
				int id = ids.get(i);
				return obufs.get(id);
			}
		}
		return null;
	}
	
	private int hashKey(int value){
		crc32.update(value);
		int v = (int)crc32.getValue();
		crc32.reset();
		return v;
	}
	
	private int hashKey(String value){
		int v = value.hashCode();
		return hashKey(v);
	}

	@Override
	public OutputBuffer route(Map<Integer, OutputBuffer> obufs) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
