package uk.ac.imperial.lsds.seep.integration.performance.microbenchmarks;

import java.nio.ByteBuffer;

public class ProducerConsumerTest {
	

	public static void main(String args[]){
	
		Buffer b = new Buffer(6);
		Thread p = new Thread(new Producer(b));
		Thread c = new Thread(new Consumer(b));
		
		c.start();
		p.start();
		
		while(true);
	}
	
}

class Producer implements Runnable{
	
	private boolean working = true;
	private Buffer b;
	
	public void stop(){
		working = false;
	}
	
	public Producer(Buffer b){
		this.b = b;
	}
	
	@Override
	public void run(){
		byte a = 0,b = 0,c = 0;
		while(working){
			byte[] data = new byte[] { a, b, c, (byte) (a*2), (byte) (b*2), (byte) (c*2)};
			a++; b++; c++;
			this.b.add(data); // will block if there's not enough space
		}
	}
	
}

class Consumer implements Runnable{
	
	private boolean working = true;
	private Buffer b;
	
	public void stop(){
		working = false;
	}
	
	public Consumer(Buffer b){
		this.b = b;
	}
	
	@Override
	public void run(){
		int counter = 0;
		long time = System.currentTimeMillis();
		while(working){
			byte[] data = b.poll();
			counter++;
			long elapsedTime = (System.currentTimeMillis()-time);
			if(elapsedTime > 1000){
				System.out.println("r/s: "+counter);
				counter = 0;
				time = System.currentTimeMillis();
			}
		}
	}
}

class Buffer{
	
	private ByteBuffer buf;
	private boolean completed;
	
	public Buffer(int batchSize){
		buf = ByteBuffer.allocate(batchSize);
	}
	
	public void add(byte[] data){
		if(completed) waitHere(); // block
		if(enoughSpaceInBuffer(data.length)) buf.put(data);
		else completed = true;
	}
	
	public byte[] poll(){
		byte[] data = null;
		if(completed){
			buf.flip();
			data = new byte[6];
			if(buf.remaining() > 6){
				buf.get(data, 0, 6);
			}
			else{ // finished reading all batch
				completed = false;
				buf.clear();
				notifyHere();
			}
		}
		return data;
	}
	
	private boolean enoughSpaceInBuffer(int size){
		return buf.remaining() > size;
	}
	
	private void notifyHere(){
		synchronized(this){
			notify();
		}
	}
	
	private void waitHere(){
		try {
			synchronized(this){
				wait();
			}
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
