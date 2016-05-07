package uk.ac.imperial.lsds.seepworker.core;

import static com.codahale.metrics.MetricRegistry.name;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;

import uk.ac.imperial.lsds.seep.metrics.SeepMetrics;
import uk.ac.imperial.lsds.seepworker.WorkerConfig;

public class BufferPool {

	final private Logger LOG = LoggerFactory.getLogger(BufferPool.class.getName());
	
	private final int minBufferSize;
	private final long totalMemAvailableToBufferPool;
	private Deque<ByteBuffer> allocatedBuffers;
		
	// Metrics
	// usedMemory is allocated memory that is currently used by some Dataset
	// Note that the total memory usage may be higher, as buffers are pooled and not
	// released immediately. However, effectively, the total memory available would be
	// the total_memory_available - usedMemory
	final private Counter usedMemory;
	
	public static BufferPool __TEMPORAL_FAKE() {
		return new BufferPool(-666);
	}
	
	private BufferPool(int a) {
		this.minBufferSize = 8192;
		this.totalMemAvailableToBufferPool = (int)(Runtime.getRuntime().totalMemory())/2;
		this.allocatedBuffers = new ArrayDeque<ByteBuffer>();
		LOG.warn("TEMPORAL-> dangling buffer pools");
		usedMemory = SeepMetrics.REG.counter(name(BufferPool.class, "total", "mem"));
	}
	
	private BufferPool(WorkerConfig wc) {
		this.minBufferSize = wc.getInt(WorkerConfig.BUFFERPOOL_MIN_BUFFER_SIZE);
		this.totalMemAvailableToBufferPool = wc.getLong(WorkerConfig.BUFFERPOOL_MAX_MEM_AVAILABLE);
		this.allocatedBuffers = new ArrayDeque<ByteBuffer>();
		LOG.info("Created new Buffer Pool with availableMemory of {} and minBufferSize of: {}", this.totalMemAvailableToBufferPool, this.minBufferSize);
		usedMemory = SeepMetrics.REG.counter(name(BufferPool.class, "event", "mem"));
	}
	
	public static BufferPool createBufferPool(WorkerConfig wc) {
		return new BufferPool(wc);
	}
	
	public int getMinimumBufferSize() {
		return this.minBufferSize;
	}
	
	/**
	 * Will return a ByteBuffer from the pool if available. If not, it will try to allocate a new ByteBuffer,
	 * an operation that will succeed if there is enough memory available. If there is not enough memory
	 * available, the method returns null
	 * @return
	 */
	public synchronized ByteBuffer borrowBuffer() {
		if(allocatedBuffers.size() > 0) {
			ByteBuffer bb = allocatedBuffers.pop();
			bb.clear();
			usedMemory.inc(minBufferSize);
			return bb;
		}
		else {
			if(enoughMemoryAvailable()){
				usedMemory.inc(minBufferSize);
				return ByteBuffer.allocate(minBufferSize);
			}
			else {
				return null;
			}
		}
	}
	
	public ByteBuffer getCacheBuffer() {
		ByteBuffer bb = ByteBuffer.allocate(minBufferSize);
		return bb;
	}
	
	public int returnBuffer(ByteBuffer buffer) {
		int freedMemory = buffer.capacity();
		usedMemory.dec(minBufferSize);
		allocatedBuffers.add(buffer);
		return freedMemory;
	}
	
	private boolean enoughMemoryAvailable() {
		// Any headroom should have been incorporated on bufferPool creation (e.g. aprox. constant mem usage on steady state)
		if(usedMemory.getCount() + minBufferSize > totalMemAvailableToBufferPool) {
			return false;
		}
		return true;
	}

	public boolean isThereXMemAvailable(long size) {
		long availableMemory = totalMemAvailableToBufferPool - usedMemory.getCount();
		if(size < availableMemory) {
			return true;
		}
		return false;
	}

	public double getPercAvailableMemory() {
		double availableMemory = totalMemAvailableToBufferPool - usedMemory.getCount();
		return availableMemory/(double)totalMemAvailableToBufferPool;
	}

}
