import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.sources.SyntheticSource;
import uk.ac.imperial.lsds.seep.api.operator.sources.SyntheticSourceConfig;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;


public class Base implements QueryComposer {
	
	private int sel;
	private int cost;
	private int isize;
	
	public Base(String[] qParams) {
		String sel = "selectivity";
		String cost = "cost";
		String isize = "isize";
		for(int i = 0; i < qParams.length; i++) {
			String token = qParams[i];
			if(token.equals(sel)) {
				this.sel = new Integer(qParams[(i+1)]);
			}
			else if(token.equals(cost)) {
				this.cost = new Integer(qParams[(i+1)]);
			}
			else if(token.equals(isize)) {
				this.isize = new Integer(qParams[(i+1)]);
			}
		}
	}
	
	@Override
	public SeepLogicalQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
		
		Properties confSync = new Properties();
		
		String size = ""+this.isize+"";
		confSync.setProperty(SyntheticSourceConfig.GENERATED_SIZE, size);
		
		SyntheticSource synSrc = SyntheticSource.newSource(0, confSync);
		LogicalOperator adderOne = queryAPI.newStatelessOperator(new Adder(), 1);
		LogicalOperator adderTwo = queryAPI.newStatelessOperator(new Adder(), 2);
		LogicalOperator branchone = queryAPI.newStatelessOperator(new Branch1(), 3);
		LogicalOperator branchtwo = queryAPI.newStatelessOperator(new Branch2(), 4);
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), 5);
		
		synSrc.connectTo(adderOne, schema, 0);
		synSrc.connectTo(adderTwo, schema, 1);
		adderOne.connectTo(branchone, 2, new DataStore(schema, DataStoreType.NETWORK));
		adderTwo.connectTo(branchtwo, 3, new DataStore(schema, DataStoreType.NETWORK));
		branchone.connectTo(snk, 4, new DataStore(schema, DataStoreType.NETWORK));
		branchtwo.connectTo(snk, 5, new DataStore(schema, DataStoreType.NETWORK));
		
		SeepLogicalQuery slq = queryAPI.build();
		slq.setExecutionModeHint(QueryExecutionMode.ALL_SCHEDULED);
		
		return slq;
	}

}
