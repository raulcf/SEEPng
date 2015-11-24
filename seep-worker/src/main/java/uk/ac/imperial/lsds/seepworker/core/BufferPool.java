package uk.ac.imperial.lsds.seepworker.core;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class BufferPool {

	private final int minBufferSize;
	private final int totalMemAvailableToBufferPool;
	private Deque<ByteBuffer> allocatedBuffers;
	
	private BufferPool(WorkerConfig wc) {
		this.minBufferSize = wc.getInt(WorkerConfig.BUFFERPOOL_MIN_BUFFER_SIZE);
		this.totalMemAvailableToBufferPool = wc.getInt(WorkerConfig.BUFFERPOOL_MAX_MEM_AVAILABLE);
		this.allocatedBuffers = new ArrayDeque<ByteBuffer>();
	}
	
	public static BufferPool createBufferPool(WorkerConfig wc) {
		return new BufferPool(wc);
	}
	
	public synchronized ByteBuffer borrowBuffer() {
		if(allocatedBuffers.size() > 0 ) {
			ByteBuffer bb = allocatedBuffers.pop();
			bb.clear();
			return bb;
		}
		else {
			// TODO: at some point we'll need to set up a ceiling here, for now just let it die...
			return ByteBuffer.allocate(minBufferSize);
		}
	}
	
	public void returnBuffer(ByteBuffer buffer) {
		allocatedBuffers.add(buffer);
	}

}
