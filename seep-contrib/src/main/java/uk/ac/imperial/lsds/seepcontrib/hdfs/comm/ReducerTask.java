package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class ReducerTask implements SeepTask{
	
	private Schema KVP;
	private ArrayList<Integer> downstreamlist;
	private Properties hc;
	private Map<Character,Integer> map=new HashMap<Character,Integer>();
	
	public void setUp() {
		map.clear();
	}
	
	public byte[] MRprocess(String text){
		if(!map.isEmpty()&&map.containsKey(text.charAt(0))){
			int count = map.get(text.charAt(0));
			map.replace(text.charAt(0), count+1);
		}
		else{
			map.put(text.charAt(0), 1);
		}
		System.out.println("Number of :"+text.charAt(0)+" is "+map.get(text.charAt(0)));
		return null;
	}
	
	public void processData(ITuple data, API api) {
			if(data==null){
				return;
			}
			String key = data.getString("key");
			int value = data.getInt("value");
			
			MRprocess(key);
			
			//byte[] d = OTuple.create(KVP, new String[]{"key", "value"}, new Object[]{key, value});		
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
	}

	@Override
	public void close() {
		

	}
}
