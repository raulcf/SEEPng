package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.util.Utils;
import uk.ac.imperial.lsds.seepcontrib.hdfs.config.HdfsConfig;
	
public class MapperTask implements SeepTask {
	
	private Schema KVP;
	private ArrayList<Integer> downstreamlist=new ArrayList<Integer>();
	private Properties hc;
	
	public void setUp() {
	}
	
	public void addDownstream(int id){
		downstreamlist.add(id);
	}
	public void setProperty(Properties p){
		this.hc = p;
	}
	public void setKVP(Schema kvp){
		KVP=kvp;
	}
	
	public byte[] MRprocess(String text){
		byte[] d = OTuple.create(KVP, new String[]{"key", "value"}, new Object[]{text, 1});
		System.out.println("data send: "+d.length);
		return d;
	}
	
	public void processData(ITuple data, API api) {
			System.out.println("data:"+data.getData());
			byte d = data.getData()[0];
			char[] c = {(char) d};
			String text =new String(c);
			byte[] MRdata = MRprocess(text);
			//Hash function.
			//if(c[0]>'b'){
				//api.sendToStreamId(downstreamlist.get(1),MRdata);
			//}
			//else{
			api.sendToStreamId(downstreamlist.get(0), MRdata);
			//}
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
	}

	@Override
	public void close() {
		

	}

}
