import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;


public class Base implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts")
												   .newField(Type.STRING, "text").build();
		
		LogicalOperator src = queryAPI.newStatelessSource(new Src(), 0);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), 2);
		
		src.connectTo(processor, 0, new DataStore(schema, DataStoreType.NETWORK));
		processor.connectTo(snk, 0, new DataStore(schema, DataStoreType.NETWORK), ConnectionType.UPSTREAM_SYNC_BARRIER);
		
		queryAPI.setInitialPhysicalInstancesForLogicalOperator(1, 2);
		
		return queryAPI.build();
	}

}
