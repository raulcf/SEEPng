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
		LogicalOperator evaluator1 = queryAPI.newStatelessOperator(new Evaluator(), 2);
		LogicalOperator evaluator2 = queryAPI.newStatelessOperator(new Evaluator(), 3);
		
		LogicalOperator choose = queryAPI.newChooseOperator(new Choose(), 3);
		
		LogicalOperator branchone = queryAPI.newStatelessOperator(new Branch1(), 3);
		
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), 5);
		
		synSrc.connectTo(adderOne, schema, 0);
		synSrc.connectTo(adderTwo, schema, 1);
		adderOne.connectTo(evaluator1, 3, new DataStore(schema, DataStoreType.NETWORK));
		adderTwo.connectTo(evaluator2, 4, new DataStore(schema, DataStoreType.NETWORK));

		evaluator1.connectTo(choose, 7, new DataStore(schema, DataStoreType.NETWORK));
		evaluator2.connectTo(choose, 8, new DataStore(schema, DataStoreType.NETWORK));
		
		choose.connectTo(branchone, 9, new DataStore(schema, DataStoreType.NETWORK));
		
		branchone.connectTo(snk, 4, new DataStore(schema, DataStoreType.NETWORK));
		
		SeepLogicalQuery slq = queryAPI.build();
		slq.setExecutionModeHint(QueryExecutionMode.ALL_SCHEDULED);
		
		return slq;
	}

}
