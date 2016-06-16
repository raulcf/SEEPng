import java.util.LinkedList;
import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.sources.SyntheticSource;
import uk.ac.imperial.lsds.seep.api.operator.sources.SyntheticSourceConfig;



public class Base implements QueryComposer {
	int connectionId = 0;
	Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();

	private int sel;
	private int cost;
	private long isize;
	private boolean incremental_choose;
	private int fanout;
	
	public Base(String[] qParams) {
		String sel = "selectivity";
		String cost = "cost";
		String isize = "isize";
		String incrementalchoose = "incchoose";
		String fanout = "fanout";
		for(int i = 0; i < qParams.length; i++) {
			String token = qParams[i];
			if(token.equals(sel)) {
				this.sel = new Integer(qParams[(i+1)]);
			}
			else if(token.equals(cost)) {
				this.cost = new Integer(qParams[(i+1)]);
			}
			else if(token.equals(isize)) {
				this.isize = new Long(qParams[(i+1)]);
			}
			else if(token.equals(incrementalchoose)){
				this.incremental_choose = new Boolean(qParams[(i+1)]);
			}
			else if(token.equalsIgnoreCase(fanout)) {
				this.fanout = new Integer(qParams[i+1]);
			}
		}
	}
	
	@Override
	public SeepLogicalQuery compose() {
		
		Properties syncConfig = new Properties();
		String size = ""+isize+"";
		syncConfig.setProperty(SyntheticSourceConfig.GENERATED_SIZE, size);
		
		// source with adder (fixed selectivity)
		SyntheticSource synSrc = SyntheticSource.newSource(0, syncConfig);
		LogicalOperator adderOne = queryAPI.newStatelessOperator(new Adder(1.0), 1);
		synSrc.connectTo(adderOne, schema, connectionId++);
		int operatorId = 2;
		// We create a choose
		LogicalOperator choose = queryAPI.newChooseOperator(new Choose(incremental_choose), operatorId++);
		
		// explore a number of ops here, branch, that are connected upstream to adder and downstream to choose
		for(int i = 0; i < fanout; i++) {
			LogicalOperator branch = queryAPI.newStatelessOperator(new Branch1(i), operatorId++);
			LogicalOperator eval = queryAPI.newStatelessOperator(new Evaluator(), operatorId++);
			adderOne.connectTo(branch, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			branch.connectTo(eval, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			eval.connectTo(choose, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
		}
		
		// Finally connect choose to sink
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), operatorId++);
		choose.connectTo(snk, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
		
		
		SeepLogicalQuery slq = queryAPI.build();
		slq.setExecutionModeHint(QueryExecutionMode.ALL_SCHEDULED);
		return slq;
	}

}
