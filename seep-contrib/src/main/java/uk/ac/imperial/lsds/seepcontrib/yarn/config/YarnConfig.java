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

	public static final String CONTAINER_MEMORY_MB = "container.memory.mb";
	private static final String CONTAINER_MEMORY_MB_DOC = "Maximum memory allowed per container";
	
	static{
		config = new ConfigDef().define(CONTAINER_MEMORY_MB, Type.STRING, Importance.HIGH, CONTAINER_MEMORY_MB_DOC);
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
