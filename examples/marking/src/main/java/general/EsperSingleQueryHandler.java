package general;

import uk.ac.imperial.lsds.seep.errors.SchemaException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.collection.ArrayBackedCollection;
import com.espertech.esper.collection.DualWorkQueue;
import com.espertech.esper.collection.UniformPair;
import com.espertech.esper.epl.agg.aggregator.AggregatorMinMax;
import com.espertech.esper.epl.agg.service.AggregationMethodRow;
import com.espertech.esper.epl.view.OutputStrategyUtil;
import com.espertech.esper.event.BaseNestableEventUtil;
import com.espertech.esper.event.EventBeanUtility;
import com.espertech.esper.event.map.MapEventBean;
import com.espertech.esper.type.MinMaxTypeEnum;
import com.espertech.esper.view.std.AddPropertyValueView;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;

public class EsperSingleQueryHandler implements SeepTask {
	
	public final static String STREAM_IDENTIFIER = "stream";
	
	private final static Logger log = LoggerFactory.getLogger(EsperSingleQueryHandler.class);

	// This map contains a list of key,class mappings, which will be
	// registered to the ESPER engine
	private Map<String,Map<String, Object>> typesPerStream = new LinkedHashMap<>();

	/*
	 * URL of the ESPER engine instance
	 */
	private String esperEngineURL = "";

	/*
	 * The ESPER engine instance used by this processor, will be fetched based
	 * on the given URL
	 */
	private EPServiceProvider epService = null;

	private Configuration configuration = new Configuration();

	/*
	 * The following is only needed to ensure that the respective classes are available in the processing methods
	 */
	private EPServiceProviderManager providerManager = new EPServiceProviderManager();
	//private MapEventBean                  bean = new MapEventBean(null);
	private MapEventBean                  bean;
	//private ArrayBackedCollection<String> coll = new ArrayBackedCollection<String>(0);
	private ArrayBackedCollection<String> coll;// = new ArrayBackedCollection<String>(0);
	//private BaseNestableEventUtil         util = new BaseNestableEventUtil();
	private BaseNestableEventUtil         util;// = new BaseNestableEventUtil();
	//private AddPropertyValueView          view = new AddPropertyValueView(null, null, null, null);
	private AddPropertyValueView          view;// = new AddPropertyValueView(null, null, null, null);
	//private UniformPair<String>           pair = new UniformPair<String>("", "");
	private UniformPair<String>           pair;// = new UniformPair<String>("", "");
	//private OutputStrategyUtil	          uti2 = new OutputStrategyUtil();
	//private EventBeanUtility              uti3 = new EventBeanUtility();
	//private DualWorkQueue<String>         queu = new DualWorkQueue<String>();
	private OutputStrategyUtil	          uti2;// = new OutputStrategyUtil();
	private EventBeanUtility              uti3;// = new EventBeanUtility();
	private DualWorkQueue<String>         queu;// = new DualWorkQueue<String>();
	//private AggregatorMinMax			  aggm = new AggregatorMinMax(MinMaxTypeEnum.MAX, null);
	private AggregatorMinMax			  aggm; 
	//private AggregationMethodRow		  aggr = new AggregationMethodRow(1l, null);
	private AggregationMethodRow		  aggr; 
	
	
	/*
	 * The actual ESPER query as String
	 */
	private String esperQuery = "";

	private Schema outSchema = null;
	private int outSchemaLength = 0;
	
	private boolean singleInputStream = true;
	private String singleInputStreamKey = "";

	private API api = null;
	
	/*
	 * The query as a statement, built from the query string
	 */
	private EPStatement statement = null;

	public EsperSingleQueryHandler() {
		
	}

	private EsperSingleQueryHandler(String query, String url, Schema outSchema) {
		this.esperQuery = query;
		this.esperEngineURL = url;
		this.outSchema = outSchema;
		this.outSchemaLength = outSchema.fields().length;
	}

	public EsperSingleQueryHandler(String query, String url, String streamKey, String[] typeBinding, Schema outSchema) {
		this(query, url, outSchema);
		this.singleInputStreamKey = streamKey;
		this.typesPerStream.put(streamKey, getTypes(typeBinding));
		//initStatement();
	}

	public EsperSingleQueryHandler(String query, String url, Map<String, String[]> typeBinding, Schema outSchema) {
		this(query, url, outSchema);
		for (String stream : typeBinding.keySet())
			this.typesPerStream.put(stream, getTypes(typeBinding.get(stream)));
		
		this.singleInputStream = this.typesPerStream.keySet().size() == 1;
		//initStatement();
	}

	/*
	 * Setup the Esper engine and bBuild the ESPER statement from the query string. Also,  
	 * register the callback to process the match results
	 */
	public void initStatement() {


		/*
		 * Init data structures
		 */
		configuration.getEngineDefaults().getThreading()
				.setInternalTimerEnabled(false);

		if (this.typesPerStream != null) {
			for (String stream : this.typesPerStream.keySet()) {
				Map<String, Object> currentTypes = this.typesPerStream.get(stream);
				log.debug("Registering data items as '{}' in esper queries...", stream);
				for (String key : currentTypes.keySet()) {
					Class<?> clazz = (Class<?>) currentTypes.get(key);
					log.debug("  * registering type '{}' for key '{}'",
							clazz.getName(), key);
				}
				configuration.addEventType(stream, currentTypes);
				log.debug("{} attributes registered.", currentTypes.size());
			}
		}
		
		/*
		 * Get the ESPER engine instance
		 */
		epService = providerManager.getProvider(esperEngineURL,
				configuration);

		/*
		 * Initialise the query statement
		 */
		if (statement != null) {
			statement.removeAllListeners();
			statement.destroy();
		}
		
		log.debug("Creating ESPER query...");
		
		/*
		 * Build the ESPER statement
		 */
		statement = epService.getEPAdministrator().createEPL(this.esperQuery);

		/*
		 * Set a listener called when statement matches
		 */
		statement.addListener(new UpdateListener() {
			@Override
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {
				if (newEvents == null) {
					// we don't care about events leaving the window (old
					// events)
					return;
				}
				for (EventBean theEvent : newEvents) {
					sendOutput(theEvent);
				}
			}
		});
		
		log.debug("Done with init: {}", this.esperQuery);
	}

	@Override
	public void setUp() {
	    this.aggm = new AggregatorMinMax(MinMaxTypeEnum.MAX, null);
	    this.aggr = new AggregationMethodRow(1l, null);
	    this.bean = new MapEventBean(null);
	    this.coll = new ArrayBackedCollection<String>(0);
	    this.util = new BaseNestableEventUtil();
	    this.view = new AddPropertyValueView(null, null, null, null);
	    this.pair = new UniformPair<String>("", "");
	    this.uti2 = new OutputStrategyUtil();
	    this.uti3 = new EventBeanUtility();
	    this.queu = new DualWorkQueue<String>();
        initStatement();
	}

	
	protected void sendOutput(EventBean out) {

		//log.debug("Query returned a new result event: {}", out);
		
		String[] fields = out.getEventType().getPropertyNames();
		Object[] objects = new Object[outSchemaLength];
				
		for (int i = 0; i < fields.length; i++) 
			objects[i] = out.get(fields[i]);
				
		byte[] processedData = OTuple.create(outSchema, fields, objects);
		log.debug("Sending output with values: {}", objects);
		api.send(processedData);
        //Type[] types = outSchema.fields();
        //OTuple o = new OTuple(outSchema);
        //o.setValues(objects);
        //api.send(o);
	}

	@Override
	public void processData(ITuple data, API api) {
		
		this.api = api;
		
		String stream = this.singleInputStreamKey;
		
		if (!this.singleInputStream) {
			stream = data.getString(STREAM_IDENTIFIER);
		}
		
		Map<String, Object> item = new LinkedHashMap<String, Object>();
		
		// Only previously defined types are available to ESPER
		for (String key : this.typesPerStream.get(stream).keySet()) {
            int idx = data.getIndexFor(key);
            Object obj = 1;
            try{
            obj = data.getInt(idx);
            } catch(SchemaException se) {
            try{
            obj = data.getLong(idx);
            }
            catch(SchemaException se1) {
            try{
            obj = data.getFloat(idx);
            }
            catch(SchemaException se2) {
            }
            }
            }
			item.put(key, obj);
			//item.put(key, data.get(key));
        }
		
		//log.debug("Sending item {} with name '{}' to esper engine", item, stream);
			
		epService.getEPRuntime().sendEvent(item, stream);		
	}


	public Map<String, Object> getTypes(String[] types) {
		Map<String, Object> result = new LinkedHashMap<>();
		for (String def : types) {
			int idx = def.indexOf(":");
			if (idx > 0) {
				String key = def.substring(0, idx);
				String type = def.substring(idx + 1);

				Class<?> clazz = classForName(type);
				if (clazz != null) {
					log.debug("Defining type class '{}' for key '{}'", key,
							clazz);
					result.put(key, clazz);
				} else {
					log.error("Failed to locate class for type '{}'!", type);
				}
			}
		}
		return result;
	}

	protected static Class<?> classForName(String name) {
		//
		// the default packages to look for classes...
		//
		String[] pkgs = new String[] { "", "java.lang" };

		for (String pkg : pkgs) {
			String className = name;
			if (!pkg.isEmpty())
				className = pkg + "." + name;

			try {
				Class<?> clazz = Class.forName(className);
				if (clazz != null)
					return clazz;
			} catch (Exception e) {
			}
		}
		return null;
	}

	@Override
	public void close() {
		if (statement != null) {
			statement.removeAllListeners();
			statement.destroy();
		}

		if (this.epService != null)
			this.epService.destroy();
	}


	@Override
	public void processDataGroup(List<ITuple> d, API api) {
		for (ITuple tuple : d)
			this.processData(tuple, api);
	}

}
