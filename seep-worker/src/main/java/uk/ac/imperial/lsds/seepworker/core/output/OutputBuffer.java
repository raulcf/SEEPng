package uk.ac.imperial.lsds.seepworker.core.output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.comm.Connection;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class OutputBuffer {
	
	private int opId;
	private Connection c;
	private int streamId;
	
	private ByteBuffer buf;
	private boolean completed;
	private int tuplesInBatch = 0;
	private int currentBatchSize = 0;
		
	public OutputBuffer(WorkerConfig wc, int opId, Connection c, int streamId){
		this.opId = opId;
		this.c = c;
		this.streamId = streamId;
		buf = ByteBuffer.allocate(wc.getInt(WorkerConfig.BATCH_SIZE));
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
		return completed;
	}
	
	public boolean write(byte[] data){
		if(completed){
			waitHere(); // block
		}
		int tupleSize = data.length;
		if(enoughSpaceInBuffer(tupleSize)){
			buf.putInt(tupleSize);
			buf.put(data);
			tuplesInBatch++;
			currentBatchSize = currentBatchSize + tupleSize + TupleInfo.TUPLE_SIZE_OVERHEAD;
			return completed;
		}
		else{
			int currentPosition = buf.position();
			int currentLimit = buf.limit();
			buf.position(TupleInfo.NUM_TUPLES_BATCH_OFFSET);
			buf.putInt(tuplesInBatch);
			buf.putInt(currentBatchSize);
			buf.position(currentPosition);
			buf.limit(currentLimit);
			buf.flip(); // leave the buffer ready to be read
			completed = true;
			return completed;
		}
	}
	
	public boolean drain(SocketChannel channel){
		boolean fullyWritten = false;
		if(completed){
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
				currentBatchSize = TupleInfo.PER_BATCH_OVERHEAD_SIZE;
				buf.clear();
				buf.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
				completed = false;
				notifyHere();
				return true;
			}
			else{
				return false;
			}
		}
		else{
			// FIXME: remove this once tested
			System.out.println("Race Condition alert");
			System.exit(0);
		}
		return fullyWritten;
	}
	
	private boolean enoughSpaceInBuffer(int size){
		return buf.remaining() > size + TupleInfo.TUPLE_SIZE_OVERHEAD;
	}
	
	private void notifyHere(){
		synchronized(this){
			notify();
		}
	}
	
	private void waitHere(){
		try {
			synchronized(this){
				while(completed){
					wait();
				}
			}
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
