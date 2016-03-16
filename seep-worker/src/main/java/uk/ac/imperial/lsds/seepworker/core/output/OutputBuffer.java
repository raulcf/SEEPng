package uk.ac.imperial.lsds.seepworker.core.output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.RuntimeEventRegister;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.EventBasedOBuffer;

public class OutputBuffer implements EventBasedOBuffer {
	
	private final int BATCH_SIZE;
	private DataReference dr;
	
	private EventAPI eAPI;
	private ByteBuffer buf;
	private AtomicBoolean completed = new AtomicBoolean(false);
	private int tuplesInBatch = 0;
	private int currentBatchSize = 0;
		
	public OutputBuffer(DataReference dr, int batchSize) {
		this.dr = dr;
		this.BATCH_SIZE = batchSize;
		int headroomSize = this.BATCH_SIZE * 2;
		buf = ByteBuffer.allocate(headroomSize);
		buf.position(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	}
	
	@Override
	public DataReference getDataReference() {
		return dr;
	}
	
	@Override
	public int id() {
		return dr.getId();
	}
	
	@Override
	public void setEventAPI(EventAPI eAPI) {
		this.eAPI = eAPI;
	}
	
	@Override
	public EventAPI getEventAPI() {
		return eAPI;
	}
	
	@Override
	public boolean drainTo(WritableByteChannel channel) {
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
				if(!success) {
					System.out.println("PROB WHEN DRAINING");
					System.exit(0);
				}
				notifyHere();
				return true;
			}
			else {
				return false;
			}
		}
		return fullyWritten;
	}

	@Override
	public boolean write(byte[] data, RuntimeEventRegister reg) {
		if(completed.get()){
			waitHere(); // block
		}
		int tupleSize = data.length;
		buf.putInt(tupleSize);
		buf.put(data);
		tuplesInBatch++;
		currentBatchSize = currentBatchSize + tupleSize + TupleInfo.TUPLE_SIZE_OVERHEAD;
		
		if(bufferIsFull()) {
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
	
	@Override
	public boolean readyToWrite(){
		return completed.get();
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

	@Override
	public void flush() {
		// TODO Auto-generated method stub
		
	}
}
