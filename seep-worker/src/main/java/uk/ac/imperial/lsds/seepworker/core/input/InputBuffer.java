package uk.ac.imperial.lsds.seepworker.core.input;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.InputAdapter;

public class InputBuffer {
	
	private ByteBuffer buffer;
	// Used only for barrier, smells like refactoring...
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
	
	public boolean canReadFullBatch(int fromPosition, int limit){
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
	
	/**
	 * Safer, probably higher-overhead method.
	 * START
	 */
	
	
	private ByteBuffer _header = ByteBuffer.allocate(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	private ByteBuffer _payload = null;
	int nTuples = 0;
	
	public void _readFrom(ReadableByteChannel channel, InputAdapter ia){
		if(_header.remaining() > 0){
			this.read(channel, _header);
		}
		if(_payload == null && !_header.hasRemaining()){
			_header.flip();
			byte control = _header.get();
			nTuples = _header.getInt();
			int payloadSize = _header.getInt(); // payload size
			_payload = ByteBuffer.allocate(payloadSize);
		}
		
		if(_payload != null){
			this.read(channel, _payload);
			if(!_payload.hasRemaining()){
				System.out.println("buf.size: "+_payload.capacity()+" ntuples: "+nTuples);
				this.forwardTuples(_payload, nTuples, ia);
				_payload = null;
				_header.clear();
				nTuples = 0;
			}
		}
	}
	
	private void read(ReadableByteChannel src, ByteBuffer dst){
		try {
			src.read(dst);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private boolean headerComplete = false;
	private ByteBuffer header = ByteBuffer.allocate(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	private int numTuples = 0;
	private ByteBuffer content;
	
	public boolean __readFrom(ReadableByteChannel channel, InputAdapter ia) {
		boolean moreToRead = true;
		int readBytes = 0;
		
		
		// Make no progress if header is not complete
		if(!headerComplete) {
			try {
				channel.read(header);
				if(header.remaining() == 0){
					headerComplete = true;
				}
				else{
					System.out.println("more data to fill header");
					return moreToRead;
				}
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Check for incomplete reads
		if(content != null){
			int fake = 9;
			fake++;
			try {
				readBytes = channel.read(content);
				if(readBytes < 0) System.out.println("ERROR HERE");
				if(content.remaining() == 0){
					moreToRead = false;
					header.clear();
					forwardTuples(content, numTuples, ia);
					content = null;
					headerComplete = false;
					return moreToRead;
				}
				else{
					return moreToRead; // still more to read
				}
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// New read
		
		// Read payload
		header.flip();
		byte control = header.get();
		numTuples = header.getInt();
		int size = header.getInt(); // payload size
		System.out.println("SIze: "+size);
		content = ByteBuffer.allocate(size); // allocate a new buffer
		readBytes = 0;
		try{
			readBytes = channel.read(content);
			if(readBytes == size) {
				moreToRead = false; // complete read
				header.clear();
				// Get tuples and forward
				forwardTuples(content, numTuples, ia);
				numTuples = 0;
				content = null;
				headerComplete = false;
				return moreToRead;
			}
			else {
				System.out.println("Read-bytes: "+readBytes+" size: "+size);
				return moreToRead;
			}
		}
		catch(IOException io){
			// TODO: implement
		}
		
		return moreToRead;
	}
	
	private void forwardTuples(ByteBuffer buf, int numTuples, InputAdapter ia) {
		buf.flip(); // Prepare buffer to read
		for(int i = 0; i < numTuples; i++){			
			int tupleSize = buf.getInt();
			byte[] completedRead = new byte[tupleSize];
			buf.get(completedRead, 0, tupleSize);
			ia.pushData(completedRead);
		}
		buf.clear();
	}
	// #######
	// END
	// #######
	
	public boolean readFrom(ReadableByteChannel channel, InputAdapter ia){
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
