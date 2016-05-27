package uk.ac.imperial.lsds.seep.perf;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class WriteToChannelWriteToBufferedOutputTest {

	public static void main(String args[]) throws IOException {
		
		// vars
		String file = "testChannel";
		String file2 = "testBuffered";
		long size = 1024*1024*1024;
		ByteBuffer bb = ByteBuffer.allocate((int) size);
		
		
		long s = System.nanoTime();
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file2), 4*1024*1024);
		byte[] payload = bb.array();
		int limit2 = bb.limit();
		bos.write(limit2);
		bos.write(payload, 0, payload.length);
		bos.flush();
		bos.close();
		long e = System.nanoTime();
		
		System.out.println("bufferedoutput: " + (e-s));
		
		// channel
		 s = System.nanoTime();
		WritableByteChannel bc = Channels.newChannel(new FileOutputStream(file, true));
		int limit = bb.limit();
		ByteBuffer limitInt = ByteBuffer.allocate(Integer.BYTES).putInt(limit);
		bc.write(limitInt);
		bc.write(bb);
		bc.close();
		 e = System.nanoTime();
		
		System.out.println("channel: " + (e-s));
		
	}
	
}
