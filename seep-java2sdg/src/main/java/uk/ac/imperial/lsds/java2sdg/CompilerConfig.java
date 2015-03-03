package uk.ac.imperial.lsds.java2sdg;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigKey;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;

public class CompilerConfig extends Config {

	private static final ConfigDef config;
    
    public static final String TARGET_OUTPUT = "target.type";
    private static final String TARGET_OUTPUT_DOC = "The target type can be a DOT file, a GEXF file or a seep-runnable JAR";
    
    public static final String OUTPUT_FILE = "output.file";
    private static final String OUTPUT_FILE_DOC = "The name of the output file";
    
    public static final String INPUT_FILE = "input.file";
    private static final String INPUT_FILE_DOC = "The name of the input java file to compile";
    
    public static final String TE_ANALYZER_TYPE = "te.analyzer.type";
    private static final String TE_ANALYZER_TYPE_DOC = "Choose the type of TE analyzer from the available strategies";
    
	
	static{
		config = new ConfigDef().define(TARGET_OUTPUT, Type.INT, 0, Importance.HIGH, TARGET_OUTPUT_DOC)
				.define(OUTPUT_FILE, Type.STRING, Importance.HIGH, OUTPUT_FILE_DOC)
				.define(INPUT_FILE, Type.STRING, Importance.HIGH, INPUT_FILE_DOC)
				.define(TE_ANALYZER_TYPE, Type.INT, Importance.HIGH, TE_ANALYZER_TYPE_DOC);
	}
	
	public CompilerConfig(Map<? extends Object, ? extends Object> originals) {
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
