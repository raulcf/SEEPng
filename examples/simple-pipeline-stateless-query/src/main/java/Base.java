import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;


public class Base implements QueryComposer {

	@Override
	public LogicalSeepQuery compose() {
		
		Schema schema1 = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts")
												   .newField(Type.STRING, "text").build();
		Schema schema2 = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
		
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), 0);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), 2);
		
		src.connectTo(processor, 0, schema1);
		processor.connectTo(snk, 0, schema1);
		
		return queryAPI.build();
	}

}
