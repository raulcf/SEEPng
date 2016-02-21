package uk.ac.imperial.lsds.seep.scheduler;

import static org.junit.Assert.*;

import java.util.PriorityQueue;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;


/**
 * @author pg1712
 *
 */
public class PriorityStageTest {
	PriorityQueue<Stage> queue = new PriorityQueue<Stage>();
	
	@Test
	public void PriotiryQueueTest(){
		PriorityQueue<Stage> queue = new PriorityQueue<Stage>();
		Random r = new Random();
		for(int i=0; i< 1000; i++){
			int randomValue = 0 + r.nextInt(Integer.MAX_VALUE);
			Stage tmp = new Stage(i, randomValue);
			queue.add(tmp);
			System.out.println("[Create] StageID:\t"+ tmp.getStageId() +"\t with priotiry \t"+ tmp.getPriority());
		}
		while(!queue.isEmpty()){
			Stage tmp = queue.remove();
			System.out.println("[Poll] StageID:\t"+ tmp.getStageId()+ "\t with priotity \t"+tmp.getPriority());
			
			if(queue.peek()!=null)
				assertFalse(tmp.getPriority() < queue.peek().getPriority());
		}
		assert(true);
	}

	
	
}
