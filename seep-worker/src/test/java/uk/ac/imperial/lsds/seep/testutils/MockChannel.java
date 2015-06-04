package uk.ac.imperial.lsds.seep.testutils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;


public class MockChannel implements ByteChannel {

		private ByteBuffer bb;
		
		public int position() { return bb.position(); }
		public int limit() {return bb.limit(); }
		public int capacity() {return bb.capacity(); }
		
		public MockChannel(ByteBuffer bb) {
			this.bb = bb;
		}
		
		@Override
		public int read(ByteBuffer dst) throws IOException {
			while(dst.hasRemaining()) {
				byte b = bb.get();
				dst.put(b);
			}
			return 0;
		}

		@Override
		public boolean isOpen() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int write(ByteBuffer src) throws IOException {
			bb.put(src);
			return 0;
		}
		
		public void flip() {
			bb.flip();
		}
		
		public boolean hasRemaining() {
			return bb.hasRemaining();
		}
		public void clear() {
			bb.clear();
		}
		
	}
