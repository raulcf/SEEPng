package uk.ac.imperial.lsds.seep.api.operator.sources;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class SyntheticSourceConfig extends Config {

	private static final ConfigDef config;

	public static final String GENERATED_SIZE = "generated.size";
	private static final String GENERATED_SIZE_DOC = "Total size of the generated data";
	
	static {
		config = new ConfigDef().define(GENERATED_SIZE, Type.INT, 1024, Importance.HIGH, GENERATED_SIZE_DOC);
	}
	
	public SyntheticSourceConfig(Map<? extends Object, ? extends Object> originals) {
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


