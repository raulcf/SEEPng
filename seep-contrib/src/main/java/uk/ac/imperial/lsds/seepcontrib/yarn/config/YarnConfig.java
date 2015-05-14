package uk.ac.imperial.lsds.seepcontrib.yarn.config;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class YarnConfig extends Config {

	private static final ConfigDef config;

	public static final String YARN_PROPERTIES_FILE = "yarn.properties.file";
	private static final String YARN_PROPERTIES_FILE_DOC = "Optional argument to indicate a properties file";
	
	public static final String YARN_CONTAINER_MEMORY_MB = "yarn.container.memory.mb";
	private static final String YARN_CONTAINER_MEMORY_MB_DOC = "Maximum memory allowed per container";
	
	public static final String YARN_CONTAINER_CPU_CORES = "yarn.container.cpu.cores";
	private static final String YARN_CONTAINER_CPU_CORES_DOC = "Maximum no of cpu cores allowed per container";
	
	public static final String YARN_WORKER_PACKAGE_PATH = "yarn.worker.package.path";
	private static final String YARN_WORKER_PACKAGE_PATH_DOC = "Relative path to the worker package";
	
	public static final String YARN_APPMASTER_PACKAGE_PATH = "yarn.appmaster.package.path";
	private static final String YARN_APPMASTER_PACKAGE_PATH_DOC = "Relative path to the app-master package";
	
	public static final String YARN_APPMASTER_LISTENING_PORT = "yarn.master.port";
    private static final String YARN_APPMASTER_LISTENING_PORT_DOC = "The port in which master will receive commands from workers";
    
    public static final String YARN_WORKER_LISTENING_PORT = "yarn.worker.port";
    private static final String YARN_WORKER_LISTENING_PORT_DOC = "The port in which workers will receive commands from the master";
    
    public static final String YARN_WORKER_DATA_PORT = "yarn.worker.data.port";
    private static final String YARN_WORKER_DATA_PORT_DOC = "The port used to receive data through the network";

    public static final String YARN_RESOURCE_MANAGER_HOSTNAME = "yarn.resourcemanager.hostname";
    private static final String YARN_RESOURCE_MANAGER_HOSTNAME_DOC = "The hostname where the resoucemanager lives.";
    
    static{
		config = new ConfigDef().define(YARN_PROPERTIES_FILE, Type.STRING, Importance.LOW, YARN_PROPERTIES_FILE_DOC)
				.define(YARN_CONTAINER_MEMORY_MB, Type.INT, 256, Importance.HIGH, YARN_CONTAINER_MEMORY_MB_DOC)
				.define(YARN_CONTAINER_CPU_CORES, Type.INT, 1, Importance.HIGH, YARN_CONTAINER_CPU_CORES_DOC)
				.define(YARN_WORKER_PACKAGE_PATH, Type.STRING, "", Importance.HIGH, YARN_WORKER_PACKAGE_PATH_DOC)
				.define(YARN_APPMASTER_PACKAGE_PATH, Type.STRING, "", Importance.HIGH, YARN_APPMASTER_PACKAGE_PATH_DOC)
				.define(YARN_APPMASTER_LISTENING_PORT, Type.INT, 3500, Importance.HIGH, YARN_APPMASTER_LISTENING_PORT_DOC)
				.define(YARN_WORKER_LISTENING_PORT, Type.INT, 3500, Importance.HIGH, YARN_WORKER_LISTENING_PORT_DOC)
				.define(YARN_WORKER_DATA_PORT, Type.INT, 5000, Importance.HIGH, YARN_WORKER_DATA_PORT_DOC)
				.define(YARN_RESOURCE_MANAGER_HOSTNAME, Type.STRING, "0.0.0.0", Importance.HIGH, YARN_RESOURCE_MANAGER_HOSTNAME_DOC);
	}
	
	
	public YarnConfig(Map<? extends Object, ? extends Object> originals) {
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
