package uk.ac.imperial.lsds.seepcontrib.hdfsconfig;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class HdfsConfig extends Config {

	private static final ConfigDef config;

	public static final String HDFS_SERVER = "hdfs.server";
	private static final String HDFS_SERVER_DOC = "A list of host/port pairs to use for establishing the initial connection to the HDFS File System.";
	
	public static final String INPUT_QUEUE_SIZE = "input.queue.size";
	private static final String INPUT_QUEUE_SIZE_DOC = " ";
	
	public static final String READING_TIME_OUT = "reading.time.out";
	private static final String READING_TIME_OUT_DOC = " ";
	
	static
	{
		config = new ConfigDef().define(HDFS_SERVER, Type.STRING, Importance.HIGH, HDFS_SERVER_DOC);
		config = new ConfigDef().define(INPUT_QUEUE_SIZE, Type.STRING, Importance.HIGH, INPUT_QUEUE_SIZE_DOC);
		config = new ConfigDef().define(READING_TIME_OUT, Type.STRING, Importance.HIGH, READING_TIME_OUT_DOC);
	}
	
	
	public HdfsConfig(Map<? extends Object, ? extends Object> originals) {
		super(config, originals);
	}

	public static ConfigKey getConfigKey(String name){
		return config.getConfigKey(name);
	}
	
	public static List<ConfigKey> getAllConfigKey(){
		return config.getAllConfigKey();
	}
	
	public static void main(String[] args) {
        System.out.println(config.toHtmlTable());
    }
}
