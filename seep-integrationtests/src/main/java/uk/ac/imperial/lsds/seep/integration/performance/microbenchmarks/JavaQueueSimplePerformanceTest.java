package uk.ac.imperial.lsds.seep.integration.performance.microbenchmarks;

import static org.junit.Assert.*;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;

public class JavaQueueSimplePerformanceTest {

	@Test
	public void test() {
		
		BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(100);
		
		Thread writer = new Thread (new Runnable() {
			public void run() {
				while(true) {
					try {
						queue.put(new Integer(0));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});
		
		Thread reader = new Thread(new Runnable() {
			long refTime = System.currentTimeMillis();
			int reportInterval = 1000; //ms
			int events = 0;
			public void run() {
				while(true){
					try {
						queue.take();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					events++;
					if(System.currentTimeMillis() - refTime > reportInterval) {
						System.out.println("e/s: "+events);
						events = 0;
						refTime = System.currentTimeMillis();
					}
				}
			}
		});
		
		writer.start();
		reader.start();
		
		try {
			reader.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assert(true);
	}

}
