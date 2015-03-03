import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.state.stateimpl.SeepMap;


public class Base implements QueryComposer {

	@Override
	public LogicalSeepQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), 0);
		LogicalOperator processor = queryAPI.newStatefulOperator(new Processor(), new SeepMap(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), 2);
		
		src.connectTo(processor, 0, schema);
		processor.connectTo(snk, 0, schema);
		
		queryAPI.setInitialPhysicalInstancesForLogicalOperator(1, 2);
		
		return queryAPI.build();
	}

}
