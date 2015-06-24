import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.state.stateimpl.SeepMap;


public class Base implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		
		LogicalOperator src = queryAPI.newStatelessSource(new Src(), 0);
		LogicalOperator processor = queryAPI.newStatefulOperator(new Processor(), new SeepMap(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), 2);
		
		src.connectTo(processor, 0, new DataStore(schema, DataStoreType.NETWORK));
		processor.connectTo(snk, 0, new DataStore(schema, DataStoreType.NETWORK));
		
		queryAPI.setInitialPhysicalInstancesForLogicalOperator(1, 2);
		
		return queryAPI.build();
	}

}
