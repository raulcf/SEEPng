package uk.ac.imperial.lsds.seepcontrib.hdfs.config;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.core.InputAdapter;

public class HdfsConfig extends Config {

	private static final ConfigDef config;

	public static final String HDFS_SERVER = "hdfs.server";
	private static final String HDFS_SERVER_DOC = "A list of host/port pairs to use for establishing the initial connection to the Hdfs cluster."
												+ "Data will be load balanced over all servers irrespective of which servers are specified here for"
												+ "bootstrapping this list only impacts the initial hosts used to discover the full set of servers."
												+ "This list should be in the form host1:port1,host2:port2,.... Since these servers are just used"
												+ "for the initial connection to discover the full cluster membership (which may change dynamically),"
												+ " this list need not contain the full set of servers (you may want more than one, though, in case"
												+ "a server is down). If no server in this list is available sending data will fail until on becomes"
												+ "available.";
	public static final String HDFS_PATH = "hdfs.path";
	private static final String HDFS_PATH_DOC = "A path for the HDFS file on the server.";
	
	public static final String HDFS_TEXT = "hdfs.text";
	private static final String HDFS_TEXT_DOC = "Check if the source is pure text format";


	static{
		config = new ConfigDef().define(HDFS_SERVER, Type.STRING, Importance.HIGH, HDFS_SERVER_DOC).define(HDFS_PATH, Type.STRING, Importance.HIGH, HDFS_PATH_DOC).define(HDFS_TEXT, Type.STRING, Importance.HIGH, HDFS_TEXT_DOC);
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