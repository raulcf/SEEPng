package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.IBuffer;
import uk.ac.imperial.lsds.seep.core.InputAdapter;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;


public class InputBuffer implements IBuffer {
	
	private DataReference dRef;
	
	private ByteBuffer header = ByteBuffer.allocate(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	private ByteBuffer payload = null;
	private int nTuples = 0;
	
	private BlockingQueue<byte[]> queue;
	private int queueSize;
	
	private InputBuffer(WorkerConfig wc, DataReference dr) {
		this.queueSize = wc.getInt(WorkerConfig.SIMPLE_INPUT_QUEUE_LENGTH);
		this.queue = new ArrayBlockingQueue<>(queueSize);
		this.dRef = dr;
	}
	
	public static InputBuffer makeInputBufferFor(WorkerConfig wc, DataReference dr) {
		return new InputBuffer(wc, dr);
	}
	
	@Override
	public DataReference getDataReference() {
		return dRef;
	}
	
	@Override
	public int readFrom(ReadableByteChannel channel) {
		int totalTuplesRead = 0;
		if(header.remaining() > 0) {
			this.read(channel, header);
		}
		
		if(payload == null && !header.hasRemaining()) {
			header.flip();
			byte control = header.get();
			nTuples = header.getInt();
			int payloadSize = header.getInt(); // payload size
			payload = ByteBuffer.allocate(payloadSize);
		}
		
		if(payload != null) {
			this.read(channel, payload);
			if(!payload.hasRemaining()) {
				totalTuplesRead = this.forwardTuples(payload, nTuples);
				payload = null;
				header.clear();
				nTuples = 0;
			}
		}
		return totalTuplesRead;
	}
	
	private int read(ReadableByteChannel src, ByteBuffer dst){
		try {
			return src.read(dst);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	private int forwardTuples(ByteBuffer buf, int numTuples) {
		int totalTuplesForwarded = 0;
		int tupleSize = 0;
		buf.flip(); // Prepare buffer to read
		for(int i = 0; i < numTuples; i++){			
			tupleSize = buf.getInt();
			byte[] completedRead = new byte[tupleSize];
			buf.get(completedRead, 0, tupleSize);
			this.pushData(completedRead);
			totalTuplesForwarded++;
		}
		buf.clear();
		return totalTuplesForwarded;
	}
	
	@Override
	public void pushData(byte[] data) {
		try {
			queue.put(data);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public byte[] read(int timeout) {
		try {
			return queue.poll(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
//	@Deprecated
//	public void readFrom(ReadableByteChannel channel, InputAdapter ia) {
//		
//		if(header.remaining() > 0){
//			this.read(channel, header);
//		}
//		
//		if(payload == null && !header.hasRemaining()){
//			header.flip();
//			byte control = header.get();
//			nTuples = header.getInt();
//			int payloadSize = header.getInt(); // payload size
//			payload = ByteBuffer.allocate(payloadSize);
//		}
//		
//		if(payload != null){
//			this.read(channel, payload);
//			if(!payload.hasRemaining()){
//				this.forwardTuples(payload, nTuples, ia);
//				payload = null;
//				header.clear();
//				nTuples = 0;
//			}
//		}
//	}
	
	/** 
	 * TODO: refactor according to the new model (above) : NetworkBarrier
	 */
	
	public InputBuffer(int size){
		buffer = ByteBuffer.allocate(size);
		completedReads = new ArrayDeque<>();
	}
	
	// Used only for barrier, smells like refactoring...
	private ByteBuffer buffer;
	private Deque<byte[]> completedReads;
	
	public boolean hasCompletedReads(){
		return completedReads.size() > 0;
	}
	
	public byte[] __read(){
		return completedReads.poll();
	}
	
	public boolean readToInternalBuffer(ReadableByteChannel channel, InputAdapter ia){
		boolean dataRemainingInBuffer = true;
		int readBytes = 0;
		try {
			readBytes = ((SocketChannel)channel).read(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int initialLimit = buffer.position();
		int fromPosition = 0;
		while(dataRemainingInBuffer){
			if(canReadFullBatch(fromPosition, initialLimit)){
				buffer.limit(initialLimit);
				buffer.position(fromPosition);
				
				byte control = buffer.get();
				int numTuples = buffer.getInt();
				int batchSize = buffer.getInt();
				for(int i = 0; i < numTuples; i++){
					int tupleSize = buffer.getInt();
					byte[] completedRead = new byte[tupleSize];
					buffer.get(completedRead, 0, tupleSize);
					completedReads.add(completedRead);
				}
				fromPosition = buffer.position(); // Update position for next iteration
			}
			else{
				if(buffer.hasRemaining()){
					buffer.compact(); // make space to complete chunked read
					return false;
				}
				else{
					dataRemainingInBuffer = false;
					buffer.clear();
					return true; // Fully read buffer
				}
			}
		}
		return false;
	}
	
	private boolean canReadFullBatch(int fromPosition, int limit){
		// Check whether we can read a complete batch

		int initialPosition = buffer.position();
		int initialLimit = buffer.limit();
		
		buffer.position(fromPosition);
		buffer.limit(limit);
		int remaining = buffer.remaining();
		if(remaining < TupleInfo.PER_BATCH_OVERHEAD_SIZE){
			// Reset buffer back to initial status and wait for more data to arrive
			buffer.limit(initialLimit);
			buffer.position(initialPosition);
			return false;
		} 
		else{
			buffer.position(fromPosition + TupleInfo.BATCH_SIZE_OFFSET);
			int batchSize = buffer.getInt();
			buffer.limit(initialLimit);
			buffer.position(initialPosition);
			if(remaining < batchSize){
				return false;
			}
			return true;
		}
	}
}
