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
	private static final String YARN_WORKER_PACKAGE_PATH_DOC = "Relative path to the worker jar package";
	
	static{
		config = new ConfigDef().define(YARN_PROPERTIES_FILE, Type.STRING, Importance.HIGH, YARN_PROPERTIES_FILE_DOC)
				.define(YARN_CONTAINER_MEMORY_MB, Type.STRING, Importance.HIGH, YARN_CONTAINER_MEMORY_MB_DOC)
				.define(YARN_CONTAINER_CPU_CORES, Type.STRING, Importance.HIGH, YARN_CONTAINER_CPU_CORES_DOC)
				.define(YARN_WORKER_PACKAGE_PATH, Type.STRING, Importance.HIGH, YARN_WORKER_PACKAGE_PATH_DOC);
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
