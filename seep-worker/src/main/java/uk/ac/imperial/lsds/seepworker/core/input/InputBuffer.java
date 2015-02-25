package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.InputAdapter;

public class InputBuffer {
	
	private ByteBuffer header = ByteBuffer.allocate(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	private ByteBuffer payload = null;
	private int nTuples = 0;
	
	// Used only for barrier, smells like refactoring...
	private ByteBuffer buffer;
	private Deque<byte[]> completedReads;
	
	public InputBuffer(int size){
		buffer = ByteBuffer.allocate(size);
		completedReads = new ArrayDeque<>();
	}
	
	public boolean hasCompletedReads(){
		return completedReads.size() > 0;
	}
	
	public byte[] read(){
		return completedReads.poll();
	}
	
	public void readFrom(ReadableByteChannel channel, InputAdapter ia) {
		
		if(header.remaining() > 0){
			this.read(channel, header);
		}
		
		if(payload == null && !header.hasRemaining()){
			header.flip();
			byte control = header.get();
			nTuples = header.getInt();
			int payloadSize = header.getInt(); // payload size
			payload = ByteBuffer.allocate(payloadSize);
		}
		
		if(payload != null){
			this.read(channel, payload);
			if(!payload.hasRemaining()){
				this.forwardTuples(payload, nTuples, ia);
				payload = null;
				header.clear();
				nTuples = 0;
			}
		}
	}
	
	private void forwardTuples(ByteBuffer buf, int numTuples, InputAdapter ia) {
		int tupleSize = 0;
		buf.flip(); // Prepare buffer to read
		for(int i = 0; i < numTuples; i++){			
			tupleSize = buf.getInt();
			byte[] completedRead = new byte[tupleSize];
			buf.get(completedRead, 0, tupleSize);
			ia.pushData(completedRead);
		}
		buf.clear();
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
	
	/** 
	 * TODO: refactor according to the new model (above)
	 */
	
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
	
	@Deprecated
	public boolean _readFrom(ReadableByteChannel channel, InputAdapter ia){
		boolean dataRemainingInBuffer = true;
		int readBytes = 0;
		try {
			readBytes = channel.read(buffer);
			if(readBytes <= 0){
				return false;
			}
		} 
		catch (IOException e) {
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
					ia.pushData(completedRead);
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
}
