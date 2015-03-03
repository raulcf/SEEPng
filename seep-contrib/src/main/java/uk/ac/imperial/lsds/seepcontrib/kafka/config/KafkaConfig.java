package uk.ac.imperial.lsds.seepcontrib.kafka.config;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class KafkaConfig extends Config {

	private static final ConfigDef config;

	public static final String KAFKA_SERVER = "kafka.server";
	private static final String KAFKA_SERVER_DOC = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster."
												+ "Data will be load balanced over all servers irrespective of which servers are specified here for"
												+ "bootstrapping this list only impacts the initial hosts used to discover the full set of servers."
												+ "This list should be in the form host1:port1,host2:port2,.... Since these servers are just used"
												+ "for the initial connection to discover the full cluster membership (which may change dynamically),"
												+ " this list need not contain the full set of servers (you may want more than one, though, in case"
												+ "a server is down). If no server in this list is available sending data will fail until on becomes"
												+ "available.";
	
	public static final String PRODUCER_CLIENT_ID = "client.id";
	private static final String PRODUCER_CLIENT_ID_DOC = "The id string to pass to the server when making requests. The purpose of this is to be able"
												+ "to track the source of requests beyond just ip/port by allowing a logical application name to be"
												+ "included with the request. The application can set any string it wants as this has no functional"
												+ "purpose other than in logging and metrics.";
	
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	private static final String ZOOKEEPER_CONNECT_DOC = "Specifies the ZooKeeper connection string in the form hostname:port, where hostname and port"
												+ "are the host and port for a node in your ZooKeeper cluster. To allow connecting through other"
												+ "ZooKeeper nodes when that host is down you can also specify multiple hosts in the form"
												+ "hostname1:port1,hostname2:port2,hostname3:port3. ZooKeeper also allows you to add a 'chroot' path"
												+ "which will make all kafka data for this cluster appear under a particular path. This is a way to"
												+ "setup multiple Kafka clusters or other applications on the same ZooKeeper cluster. To do this give"
												+ "a connection string in the form hostname1:port1,hostname2:port2,hostname3:port3/chroot/path which"
												+ "would put all this cluster's data under the path /chroot/path. Note that you must create this path"
												+ "yourself prior to starting the broker and consumers must use the same connection string."; 
	
	public static final String CONSUMER_GROUP_ID = "group.id";
	private static final String CONSUMER_GROUP_ID_DOC = "A string that uniquely identifies the group of consumer processes to which this consumer"
											    + "belongs. By setting the same group id multiple processes indicate that they are all part of the"
											    + "same consumer group.";
	
	public static final String BASE_TOPIC = "topic";
	private static final String BASE_TOPIC_DOC = "The common base shared by all topics names used to communicate between operators. Every unique stream"
												+ "will use topic + stream id as its unique topic";
	
	static{
		config = new ConfigDef().define(KAFKA_SERVER, Type.STRING, Importance.HIGH, KAFKA_SERVER_DOC)
				.define(PRODUCER_CLIENT_ID, Type.STRING, Importance.HIGH, PRODUCER_CLIENT_ID_DOC)
				.define(ZOOKEEPER_CONNECT, Type.STRING, "seep", Importance.HIGH, ZOOKEEPER_CONNECT_DOC)
				.define(CONSUMER_GROUP_ID, Type.STRING, "seep", Importance.HIGH, CONSUMER_GROUP_ID_DOC)
				.define(BASE_TOPIC, Type.STRING, "seep", Importance.HIGH, BASE_TOPIC_DOC);
	}
	
	
	public KafkaConfig(Map<? extends Object, ? extends Object> originals) {
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
