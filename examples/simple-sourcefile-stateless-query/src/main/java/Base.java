import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.FileConfig;
import uk.ac.imperial.lsds.seep.api.FileSource;
import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.LogicalSeepQuery;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;


public class Base implements QueryComposer {

	@Override
	public LogicalSeepQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "param1").newField(Type.INT, "param2").build();
		
		Properties p = new Properties();
		p.setProperty(FileConfig.FILE_PATH, "test.txt");
		p.setProperty(FileConfig.SERDE_TYPE, new Integer(SerializerType.NONE.ofType()).toString());
		
		FileSource fileSource = FileSource.newSource(0, p);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), 2);
		
		fileSource.connectTo(processor, 0, schema);
		processor.connectTo(snk, 0, schema);
		
		return queryAPI.build();
	}

}