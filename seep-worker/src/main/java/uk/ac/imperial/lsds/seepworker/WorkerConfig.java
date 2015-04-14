package uk.ac.imperial.lsds.seepworker;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class WorkerConfig extends Config {

    private static final ConfigDef config;
	
    public static final String PROPERTIES_FILE = "properties.file";
    public static final String PROPERTIES_RESOURCE_FILE = "config.properties";
    private static final String PROPERTIES_FILE_DOC = "Optional argument to indicate a properties file";
	
	public static final String DEPLOYMENT_TARGET_TYPE = "deployment_target.type";
    private static final String DEPLOYMENT_TARGET_TYPE_DOC = "The target cluster to which the master will submit queries."
    													+ "Physical cluster(0), yarn container(1), lxc, docker, etc";
    
    public static final String LISTENING_PORT = "worker.port";
    private static final String LISTENING_PORT_DOC = "The port in which workers will receive commands from the master";
    
    public static final String DATA_PORT = "data.port";
    private static final String DATA_PORT_DOC = "The port used to receive data through the network";
    
    public static final String MASTER_IP = "master.ip";
    private static final String MASTER_IP_DOC = "The Ip where the master is listening";
    
    public static final String MASTER_PORT = "master.port";
    private static final String MASTER_PORT_DOC = "The port where the master is listening";
    
    public static final String MASTER_CONNECTION_RETRIES = "master.connection.retries.number";
    private static final String MASTER_CONNECTION_RETRIES_DOC = "Maximum number of attemps to connect to master";
    
    public static final String MASTER_RETRY_BACKOFF_MS = "master.retry.backoff.ms";
    private static final String MASTER_RETRY_BACKOFF_MS_DOC = "Time between retries when reconnecting to master";
    
    public static final String ENGINE_TYPE = "engine.type";
    private static final String ENGINE_TYPE_DOC = "Defines the type of processing engine that will process data";
    
    public static final String NUM_NETWORK_READER_THREADS = "network.reader.threads.num";
    private static final String NUM_NETWORK_READER_THREADS_DOC = "Total number of threads responsible for reading from the network";
    
    public static final String NUM_NETWORK_WRITER_THREADS = "network.writer.threads.num";
    private static final String NUM_NETWORK_WRITER_THREADS_DOC = "Total number of threads responsible for writing to the network";
    
    public static final String MAX_PENDING_NETWORK_CONNECTION_PER_THREAD = "max.pending.connection.thread";
    private static final String MAX_PENDING_NETWORK_CONNECTION_PER_THREAD_DOC = "Max. number of pending connections per thread";
    
    public static final String SIMPLE_INPUT_QUEUE_LENGTH = "simple.input.queue.length";
    private static final String SIMPLE_INPUT_QUEUE_LENGTH_DOC = "The length of a simple input queue, in case this is configured";
    
    public static final String BATCH_SIZE = "batch.size";
    private static final String BATCH_SIZE_DOC = "Recommended maximum batch size in bytes. Note that this is not enforced, the system"
    													+ "will try to achieve this size on a best effort basis";
    
    public static final String APP_BATCH_SIZE = "app.batch.size";
    private static final String APP_BATCH_SIZE_DOC = "The total number of tuples batched at the application level";

    public static final String MAX_WAIT_TIME_PER_INPUTADAPTER_MS = "wait.time.inputadapter.ms";
    private static final String MAX_WAIT_TIME_PER_INPUTADAPTER_MS_DOC = "Maximum time an engine will poll a given inputadapter"
    													+ "for data. It only applies to certain inputAdapters.";
    
    public static final String REPORT_METRICS_CONSOLE_PERIOD = "metrics.report.console.period";
    private static final String REPORT_METRICS_CONSOLE_PERIOD_DOC = "Positive number to indicate after how many seconds to report metrics"
    													+ "via the console, or -1|0 to not report via console";
    
    public static final String REPORT_METRICS_JMX= "metrics.report.jmx";
    private static final String REPORT_METRICS_JMX_DOC = "Activate JMX reporting. Requires MBeans plugin in visualVM to visualize";
	
	static{
		config = new ConfigDef().define(DEPLOYMENT_TARGET_TYPE, Type.INT, 0, Importance.HIGH, DEPLOYMENT_TARGET_TYPE_DOC)
				.define(LISTENING_PORT, Type.INT, 3500, Importance.HIGH, LISTENING_PORT_DOC)
				.define(MASTER_PORT, Type.INT, 3500, Importance.HIGH, MASTER_PORT_DOC)
				.define(MASTER_IP, Type.STRING, Importance.HIGH, MASTER_IP_DOC)
				.define(MASTER_CONNECTION_RETRIES, Type.INT, Integer.MAX_VALUE, Importance.LOW, MASTER_CONNECTION_RETRIES_DOC)
				.define(MASTER_RETRY_BACKOFF_MS, Type.INT, 3000, Importance.LOW, MASTER_RETRY_BACKOFF_MS_DOC)
				.define(ENGINE_TYPE, Type.INT, 0, Importance.MEDIUM, ENGINE_TYPE_DOC)
				.define(DATA_PORT, Type.INT, 4500, Importance.MEDIUM, DATA_PORT_DOC)
				.define(NUM_NETWORK_READER_THREADS, Type.INT, 2, Importance.MEDIUM, NUM_NETWORK_READER_THREADS_DOC)
				.define(NUM_NETWORK_WRITER_THREADS, Type.INT, 2, Importance.MEDIUM, NUM_NETWORK_WRITER_THREADS_DOC)
				.define(MAX_PENDING_NETWORK_CONNECTION_PER_THREAD, Type.INT, 10, Importance.LOW, MAX_PENDING_NETWORK_CONNECTION_PER_THREAD_DOC)
				.define(SIMPLE_INPUT_QUEUE_LENGTH, Type.INT, 100, Importance.MEDIUM, SIMPLE_INPUT_QUEUE_LENGTH_DOC)
				.define(BATCH_SIZE, Type.INT, 100, Importance.HIGH, BATCH_SIZE_DOC)
				.define(APP_BATCH_SIZE, Type.INT, 100, Importance.HIGH, APP_BATCH_SIZE_DOC)
				.define(MAX_WAIT_TIME_PER_INPUTADAPTER_MS, Type.INT, 500, Importance.MEDIUM, MAX_WAIT_TIME_PER_INPUTADAPTER_MS_DOC)
				.define(REPORT_METRICS_CONSOLE_PERIOD, Type.INT, -1, Importance.LOW, REPORT_METRICS_CONSOLE_PERIOD_DOC)
				.define(REPORT_METRICS_JMX, Type.INT, 1, Importance.MEDIUM, REPORT_METRICS_JMX_DOC)
				.define(PROPERTIES_FILE, Type.STRING, Importance.LOW, PROPERTIES_FILE_DOC);
	}
	
	public WorkerConfig(Map<? extends Object, ? extends Object> originals) {
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
