package uk.ac.imperial.lsds.seepmaster.scheduler;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.scheduler.Stage;

public class StageTracker {

	final private Logger LOG = LoggerFactory.getLogger(StageTracker.class);
	
	private final Stage stage;
	private final Set<Integer> euInvolved;
	private final CountDownLatch countDown;
	private Set<Integer> completed;
	
	public StageTracker(Stage stage, Set<Integer> euInvolved) {
		this.stage = stage;
		this.euInvolved = euInvolved;
		this.countDown = new CountDownLatch(euInvolved.size());
		this.completed = new HashSet<>();
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
	
	public void notifyOk(int euId, int stageId) {
		if(stage.getStageId() != stageId) {
			System.out.println("ERROR, notifying for non-current stage");
			System.exit(-1);
		}
		boolean wasNotPresent = completed.add(euId);
		if(! wasNotPresent){
			LOG.warn("Notified {} that was already present", euId);
		}
		else{
			countDown.countDown();
		}
	}

}
