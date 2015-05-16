package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.DataReference;


public class StageTracker {

	final private Logger LOG = LoggerFactory.getLogger(StageTracker.class);
	
	private final int stageId;
	private Set<Integer> euInvolved;
	private final CountDownLatch countDown;
	private Set<Integer> completed;
	private Set<DataReference> results;
	
	public StageTracker(int stageId, Set<Integer> euInvolved) {
		this.stageId = stageId;
		this.euInvolved = euInvolved;
		this.countDown = new CountDownLatch(euInvolved.size());
		this.completed = new HashSet<>();
		this.results = new HashSet<>();
	}
	
	public Set<DataReference> getStageResults() {
		return results;
	}
	
	public void await(){
		try {
			countDown.await();
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void notifyOk(int euId, int stageId, Set<DataReference> partialResults) {
		if(this.stageId != stageId) {
			System.out.println("ERROR, notifying for non-current stage");
			System.exit(-1);
		}
		boolean wasNotPresent = completed.add(euId);
		if(! wasNotPresent) {
			LOG.warn("Notified {} that was already present", euId);
		}
		else{
			results.addAll(partialResults);
			countDown.countDown();
		}
	}

	public boolean finishedSuccessfully() {
		return completed.containsAll(euInvolved);
	}

}
