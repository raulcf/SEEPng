package uk.ac.imperial.lsds.seepmaster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seepmaster.query.InvalidLifecycleStatusException;
import uk.ac.imperial.lsds.seepmaster.query.QueryManager;

public class LifecycleManager {

	final private Logger LOG = LoggerFactory.getLogger(QueryManager.class);
	
	public enum AppStatus {
		MASTER_READY(0, new int[]{1, 0}),
		QUERY_SUBMITTED(1, new int[]{2, 1, 0}),
		QUERY_DEPLOYED(2, new int[]{4, 1}),
		QUERY_RUNNING(4, new int[]{5, 6, 2}),
		QUERY_FAILED(5, new int[]{6, 4, 2}),
		QUERY_STOPPED(6, new int[]{2});
		
		private int id;
		private int[] validStateTransitions;
		
		AppStatus(int id, int... validStateTransitions){
			this.id = id;
			this.validStateTransitions = validStateTransitions;
		}
		
		public int id(){
			return id;
		}
		
		public boolean canTransitTo(AppStatus appStatus){
			for(int i : validStateTransitions){
				if(i == appStatus.id()){
					return true;
				}
			}
			return false;
		}
	}
	
	
	private AppStatus status = AppStatus.MASTER_READY;
	private static LifecycleManager instance;
	
	private LifecycleManager(){	}
	
	public static LifecycleManager getInstance(){
		if(instance == null){
			return new LifecycleManager();
		}
		else{
			return instance;
		}
	}

	public boolean canTransitTo(AppStatus newStatus) {
		return status.canTransitTo(newStatus);
	}

	public void tryTransitTo(AppStatus newStatus) {
		if(canTransitTo(newStatus)){
			LOG.info(status.toString()+" => "+newStatus.toString());
			this.status = newStatus;
		}
		else{
			throw new InvalidLifecycleStatusException("Attempt to violate app lifecycle when transitioining to QUERY_SUBMITTED");
		}
	}
}
