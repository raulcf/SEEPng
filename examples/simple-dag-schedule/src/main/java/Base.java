import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.sources.SyntheticSource;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;


public class Base implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
		
		SyntheticSource synSrc = SyntheticSource.newSource(0, null);
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
