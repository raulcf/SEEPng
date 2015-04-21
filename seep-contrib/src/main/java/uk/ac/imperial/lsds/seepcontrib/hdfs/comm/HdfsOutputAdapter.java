package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.data.DataItem;
import uk.ac.imperial.lsds.seep.core.EventAPI;
import uk.ac.imperial.lsds.seep.core.OutputAdapter;
import uk.ac.imperial.lsds.seep.core.OutputBuffer;

public class HdfsOutputAdapter implements OutputAdapter {
	
	final private DataStoreType TYPE = DataStoreType.HDFS;
	private int streamId;
	private String Path;
	private FSDataOutputStream out;
	int count = 0;
	int page = 0;
	private FileSystem fs;
	public HdfsOutputAdapter(String path) {
		Path = "hdfs://"+path;
		System.out.println("The path is:"+Path);
		try {
			fs = FileSystem.get(new URI(Path),new Configuration());
				out = fs.create(new Path(Path));	
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void send(byte[] o) {
		try {
			if(count<1500)
			{
			count++;
			out.write(o);
			}
			else{
				out.flush();
				out.close();
				page++;
				count=0;
				out = fs.create(new Path(Path+"-"+page));
				count++;
				out.write(o);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}// TODO Auto-generated method stub
		
	}

	@Override
	public void sendAll(byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendKey(byte[] o, int key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendKey(byte[] o, String key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendToStreamId(int streamId, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendToAllInStreamId(int streamId, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, int key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendStreamidKey(int streamId, byte[] o, String key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void send_index(int index, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void send_opid(int opId, byte[] o) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getStreamId() {
		// TODO Auto-generated method stub
		return streamId;
	}

	@Override
	public Map<Integer, OutputBuffer> getOutputBuffers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setEventAPI(EventAPI eAPI) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataStoreType getDataOriginType() {
		// TODO Auto-generated method stub
		return TYPE;
	}

}
