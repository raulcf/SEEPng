package uk.ac.imperial.lsds.seepworker.core.output.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class ConsistentHashingRoutingState implements Router {

	private CRC32 crc32;	
	// each downstream id
	private List<Integer> opIds;
	// subspace per downstream id
	private List<Integer> subspaceFrontiers;
	
	public ConsistentHashingRoutingState(List<Integer> opIds){
		this.crc32 = new CRC32();
		this.opIds = opIds;
		this.subspaceFrontiers = new ArrayList<>();
		int numSpaces = opIds.size();
		// calculate span of each subrange of the space
		long entireSpace = (long)Integer.MAX_VALUE * 2;
		long initialSubspaceSize = (numSpaces > 0) ? entireSpace/numSpaces : entireSpace;

		int frontier = Integer.MAX_VALUE;
		for(int i = 0; i < numSpaces; i++){
			subspaceFrontiers.add(frontier);
			frontier -= initialSubspaceSize;
		}
	}

	@Override
	public int route(int key) {
		int hashedKey = hashKey(key);
		int subspaceFrontiersSize = subspaceFrontiers.size();
		for(int i = subspaceFrontiersSize-1; i >= 0; i--){
			int frontier = subspaceFrontiers.get(i);
			if(hashedKey <= frontier){
				return opIds.get(i);
			}
		}
		return -1;
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
	public int route() {
		// TODO Auto-generated method stub
		return -1;
	}
	
}
