package uk.ac.imperial.lsds.seep.api;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class FileConfig extends Config {

	private static final ConfigDef config;

	public static final String FILE_PATH = "file.path";
	private static final String FILE_PATH_DOC = "The absolute path to a file in the local filesystem";
	
	public static final String SERDE_TYPE = "serde.type";
	private static final String SERDE_TYPE_DOC = "The type of the serializer/deserializer. See SerializerType enum for reference";
	
	static {
		config = new ConfigDef().define(FILE_PATH, Type.STRING, Importance.HIGH, FILE_PATH_DOC)
								.define(SERDE_TYPE, Type.INT, 0, Importance.HIGH, SERDE_TYPE_DOC);
	}
	
	public FileConfig(Map<? extends Object, ? extends Object> originals) {
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

