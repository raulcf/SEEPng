package uk.ac.imperial.lsds.seep.api.operator.sources;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
	
	public static final String TEXT_SOURCE = "text.source";
	private static final String TEXT_SOURCE_DOC = "True if the source is text (one line per record). False if the input is binary.";
	
	public static final String CHARACTER_SET = "character.set";
	private static final String CHARACTER_SET_DOC = "Which character set to use for text input. Defaults to Charset.defaultCharset() (JVM default).";
	
	static {
		config = new ConfigDef().define(FILE_PATH, Type.STRING, Importance.HIGH, FILE_PATH_DOC)
								.define(SERDE_TYPE, Type.INT, 0, Importance.HIGH, SERDE_TYPE_DOC)
								.define(TEXT_SOURCE, Type.BOOLEAN, false, Importance.MEDIUM, TEXT_SOURCE_DOC)
								.define(CHARACTER_SET, Type.STRING, Charset.defaultCharset().name(), Importance.LOW, CHARACTER_SET_DOC);
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

