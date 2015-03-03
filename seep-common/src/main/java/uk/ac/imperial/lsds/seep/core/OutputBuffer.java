package uk.ac.imperial.lsds.seep.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.comm.Connection;

public class OutputBuffer {
	
	private final int BATCH_SIZE;
	
	private int opId;
	private Connection c;
	private int streamId;
	
	private ByteBuffer buf;
	private AtomicBoolean completed = new AtomicBoolean(false);
	private int tuplesInBatch = 0;
	private int currentBatchSize = 0;
		
	public OutputBuffer(int opId, Connection c, int streamId, int batchSize){
		this.opId = opId;
		this.c = c;
		this.streamId = streamId;
		this.BATCH_SIZE = batchSize;
		int headroomSize = this.BATCH_SIZE * 2;
		buf = ByteBuffer.allocate(headroomSize);
		buf.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	}
	
	public int getStreamId(){
		return streamId;
	}
	
	public int id(){
		return opId;
	}
	
	public Connection getConnection(){
		return c;
	}
	
	public boolean ready(){
		return completed.get();
	}

	public boolean write(byte[] data){
		
		if(completed.get()){
			waitHere(); // block
		}
		int tupleSize = data.length;
		buf.putInt(tupleSize);
		buf.put(data);
		tuplesInBatch++;
		currentBatchSize = currentBatchSize + tupleSize + TupleInfo.TUPLE_SIZE_OVERHEAD;
		
		if(bufferIsFull()){
			int currentPosition = buf.position();
			int currentLimit = buf.limit();
			buf.position(TupleInfo.NUM_TUPLES_BATCH_OFFSET);
			buf.putInt(tuplesInBatch);
			buf.putInt(currentBatchSize);
			buf.position(currentPosition);
			buf.limit(currentLimit);
			buf.flip(); // leave the buffer ready to be read
			boolean success = completed.compareAndSet(false, true);
			if(!success){
				System.out.println("PROB when writing");
				System.exit(0);
			}
		}
		return completed.get();
	}
	
	public boolean drain(SocketChannel channel){
		boolean fullyWritten = false;
		if(completed.get()){
			int totalBytesToWrite = buf.remaining();
			int writtenBytes = 0;
			try {
				writtenBytes = channel.write(buf);
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			if(writtenBytes == totalBytesToWrite){
				// prepare buffer to be filled again
				tuplesInBatch = 0;
				currentBatchSize = 0; //TupleInfo.PER_BATCH_OVERHEAD_SIZE;
				buf.clear();
				buf.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
				boolean success = completed.compareAndSet(true, false);
				if(!success){
					System.out.println("PROB WHEN DRAINING");
					System.exit(0);
				}
				notifyHere();
				return true;
			}
			else{
				return false;
			}
		}
		return fullyWritten;
	}
	
	private boolean bufferIsFull(){
		return buf.position() >= BATCH_SIZE;
	}
	
	private void notifyHere(){
		synchronized(this){
			notify();
		}
	}
	
	private void waitHere(){
		try {
			synchronized(this){
				while(completed.get()){
					wait();
				}
			}
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
