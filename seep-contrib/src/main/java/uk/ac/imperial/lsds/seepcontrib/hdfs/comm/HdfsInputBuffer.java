package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Deque;

import uk.ac.imperial.lsds.seep.api.data.TupleInfo;
import uk.ac.imperial.lsds.seep.core.InputAdapter;

public class HdfsInputBuffer {
	private ByteBuffer header = ByteBuffer.allocate(TupleInfo.PER_BATCH_OVERHEAD_SIZE);
	private ByteBuffer payload = null;
	private int nTuples = 0;
	private ByteBuffer buffer;
	private Deque<byte[]> completedReads;
	public HdfsInputBuffer(int size){
		buffer = ByteBuffer.allocate(size);
		completedReads = new ArrayDeque<>();
	}
	public boolean hasCompletedReads(){
		return completedReads.size() > 0;
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
}