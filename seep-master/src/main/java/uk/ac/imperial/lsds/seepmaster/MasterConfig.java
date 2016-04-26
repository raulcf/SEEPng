package uk.ac.imperial.lsds.seepmaster;

import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.config.Config;
import uk.ac.imperial.lsds.seep.config.ConfigDef;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Importance;
import uk.ac.imperial.lsds.seep.config.ConfigDef.Type;
import uk.ac.imperial.lsds.seep.config.ConfigKey;

public class MasterConfig extends Config {

	private static final ConfigDef config;
	
	public static final String QUERY_FILE = "query.file";
	private static final String QUERY_FILE_DOC = "The file where user queries are specified";
	
	public static final String BASECLASS_NAME = "baseclass.name";
	private static final String BASECLASS_NAME_DOC = "The name of the Base class where the query is composed";
	
	public static final String COMPOSE_METHOD_NAME = "compose.method.name";
	private static final String COMPOSE_METHOD_NAME_DOC = "Name of composing method in Base class. May get fixed in the future";
	
	public static final String QUERY_TYPE = "query.type";
	private static final String QUERY_TYPE_DOC = "Identifies the type of query that is being submitted. A traditional SEEP query (0) or a handcrafted schedule (1).";
	
	public static final String DEPLOYMENT_TARGET_TYPE = "deployment_target.type";
    private static final String DEPLOYMENT_TARGET_TYPE_DOC = "The target cluster to which the master will submit queries."
    													+ "Physical cluster(0), yarn container(1), lxc, docker, etc";
    public static final String CONTROL_PORT = "master.port";
    private static final String CONTROL_PORT_DOC = "Port that listens to commands from workers";
    
    public static final String UI_TYPE = "ui.type";
    private static final String UI_TYPE_DOC = "The type of UI chosen, simpleconsole(0), console(1), web(2), etc";
    
    public static final String SCHED_STRATEGY = "scheduling.strategy.type";
    private static final String SCHED_STRATEGY_DOC = "The scheduling strategy for scheduled queries: sequential(0), mdf(3) etc";
    
    public static final String MEM_MANAGEMENT_POLICY = "memory.management.policy";
    private static final String MEM_MANAGEMENT_POLICY_DOC = "The mem mng policy to rank datasets according to their priority to live in memory";
    
    public static final String SCHED_STAGE_ASSIGMENT_STRATEGY = "scheduling.stageassignment.type";
    private static final String SCHED_STAGE_ASSIGMENT_STRATEGY_DOC = "Choose the strategy to assign work to workers";
    
    public static final String PROPERTIES_FILE = "properties.file";
    public static final String PROPERTIES_RESOURCE_FILE = "config.properties";
    private static final String PROPERTIES_FILE_DOC = "Optional argument to indicate a properties file";

	public static final String DISK_MEM_RATIO = "diskmem.ratio";
	private static final String DISK_MEM_RATIO_DOC = "Ratio between writing/reading to disk and memory";

	static{
		config = new ConfigDef().define(QUERY_FILE, Type.STRING, "", Importance.HIGH, QUERY_FILE_DOC)
				.define(BASECLASS_NAME, Type.STRING, "", Importance.HIGH, BASECLASS_NAME_DOC) 
				.define(COMPOSE_METHOD_NAME, Type.STRING, "compose", Importance.LOW, COMPOSE_METHOD_NAME_DOC)
				.define(QUERY_TYPE, Type.INT, 0, Importance.HIGH, QUERY_TYPE_DOC)
				.define(DEPLOYMENT_TARGET_TYPE, Type.INT, 0, Importance.HIGH, DEPLOYMENT_TARGET_TYPE_DOC)
				.define(CONTROL_PORT, Type.INT, 3500, Importance.HIGH, CONTROL_PORT_DOC)
				.define(UI_TYPE, Type.INT, 0, Importance.HIGH, UI_TYPE_DOC)
				.define(SCHED_STRATEGY, Type.INT, 0, Importance.LOW, SCHED_STRATEGY_DOC)
				.define(MEM_MANAGEMENT_POLICY, Type.INT, 0, Importance.LOW, MEM_MANAGEMENT_POLICY_DOC)
				.define(PROPERTIES_FILE, Type.STRING, Importance.LOW, PROPERTIES_FILE_DOC)
				.define(SCHED_STAGE_ASSIGMENT_STRATEGY, Type.INT, 0, Importance.MEDIUM, SCHED_STAGE_ASSIGMENT_STRATEGY_DOC)
				.define(DISK_MEM_RATIO, Type.DOUBLE, 1.0, Importance.MEDIUM, DISK_MEM_RATIO_DOC);
	}
	
	public MasterConfig(Map<? extends Object, ? extends Object> originals) {
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
