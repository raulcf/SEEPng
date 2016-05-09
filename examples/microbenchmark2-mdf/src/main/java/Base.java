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
	int operatorId = 0;
	int connectionId = 0;
	Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();

	private int sel;
	private int cost;
	private long isize;
	private boolean incremental_choose;
	private int fanout1;
	private int fanout2;
	
	public Base(String[] qParams) {
		String sel = "selectivity";
		String cost = "cost";
		String isize = "isize";
		String incrementalchoose = "incchoose";
		String fanout = "fanout";
		String fanout2 = "fanout2";
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
				this.fanout1 = new Integer(qParams[i+1]);
			}
			else if(token.equalsIgnoreCase(fanout2)) {
				this.fanout2 = new Integer(qParams[i+1]);
			}
		}
	}
	
	@Override
	public SeepLogicalQuery compose() {
		
		Properties syncConfig = new Properties();
		String size = ""+isize+"";
		syncConfig.setProperty(SyntheticSourceConfig.GENERATED_SIZE, size);
		
		// source with adder (fixed selectivity)
		SyntheticSource synSrc = SyntheticSource.newSource(operatorId++, syncConfig);
		LogicalOperator adderOne = queryAPI.newStatelessOperator(new Adder(1.0), operatorId++);
		synSrc.connectTo(adderOne, schema, connectionId++);
		
		// We create a choose
		LogicalOperator outerChoose = queryAPI.newChooseOperator(new Choose(incremental_choose), operatorId++);
		
		for(int i = 0; i < fanout1; i++) {
			LogicalOperator branch = queryAPI.newStatelessOperator(new Branch1(i), operatorId++);
			adderOne.connectTo(branch, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			
			// Choose for internal branch
			LogicalOperator choose = queryAPI.newChooseOperator(new Choose(incremental_choose), operatorId++);
			for(int j = 0; j < fanout2; j++) {
				LogicalOperator branch2 = queryAPI.newStatelessOperator(new Branch1(j), operatorId++);
				LogicalOperator eval2 = queryAPI.newStatelessOperator(new Evaluator(), operatorId++);
				branch.connectTo(branch2, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
				branch2.connectTo(eval2, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
				eval2.connectTo(choose, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			}
			
			LogicalOperator eval = queryAPI.newStatelessOperator(new Evaluator(), operatorId++);
			choose.connectTo(eval, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
			eval.connectTo(outerChoose, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
		}
		
		// Finally connect choose to sink
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), operatorId++);
		outerChoose.connectTo(snk, connectionId++, new DataStore(schema, DataStoreType.NETWORK));
		
		
		SeepLogicalQuery slq = queryAPI.build();
		slq.setExecutionModeHint(QueryExecutionMode.ALL_SCHEDULED);
		return slq;
	}
	
}
