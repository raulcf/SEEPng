package uk.ac.imperial.lsds.seepmaster;

import static org.junit.Assert.*;

import org.junit.Test;

import uk.ac.imperial.lsds.seepmaster.LifecycleManager.AppStatus;

public class LifecycleManagerTest {

	@Test
	public void test() {
		LifecycleManager lifeManager = LifecycleManager.getInstance();
		
		boolean allowed = false;
		
		// From MASTER_READY
		allowed = lifeManager.canTransitTo(AppStatus.MASTER_READY);
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_DEPLOYED);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_FAILED);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_RUNNING);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_STOPPED);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_SUBMITTED);
		assertTrue(allowed == true);
		lifeManager.tryTransitTo(AppStatus.QUERY_SUBMITTED);
		
		// From QUERY_SUBMITTED
		allowed = lifeManager.canTransitTo(AppStatus.MASTER_READY);
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_DEPLOYED); 
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_SUBMITTED); // can overwrite submitted query
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_FAILED);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_RUNNING);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_STOPPED);
		assertTrue(allowed == false);
		lifeManager.tryTransitTo(AppStatus.QUERY_DEPLOYED);
		
		// From QUERY_DEPLOYED
		allowed = lifeManager.canTransitTo(AppStatus.MASTER_READY);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_DEPLOYED); // cannot redeploy
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_SUBMITTED); // can overwrite submitted query
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_FAILED);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_RUNNING);
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_STOPPED);
		assertTrue(allowed == false);
		lifeManager.tryTransitTo(AppStatus.QUERY_RUNNING);
		
		// From QUERY_RUNNING
		allowed = lifeManager.canTransitTo(AppStatus.MASTER_READY);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_DEPLOYED); // cannot redeploy
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_SUBMITTED); // can overwrite submitted query
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_FAILED);
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_RUNNING); // must stop before running again
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_STOPPED);
		assertTrue(allowed == true);
		lifeManager.tryTransitTo(AppStatus.QUERY_STOPPED);
		
		// From QUERY_STOPPED
		allowed = lifeManager.canTransitTo(AppStatus.MASTER_READY);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_DEPLOYED); // cannot redeploy
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_SUBMITTED); // can overwrite submitted query
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_FAILED);
		assertTrue(allowed == false);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_RUNNING); // must stop before running again
		assertTrue(allowed == true);
		allowed = lifeManager.canTransitTo(AppStatus.QUERY_STOPPED);
		assertTrue(allowed == false);
		
	}

}
