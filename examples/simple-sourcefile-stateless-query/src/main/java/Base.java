import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.operator.sources.FileConfig;
import uk.ac.imperial.lsds.seep.api.operator.sources.FileSource;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.comm.serialization.SerializerType;


public class Base implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "param1").newField(Type.INT, "param2").build();
		
		Properties p = new Properties();
		p.setProperty(FileConfig.FILE_PATH, "test.txt");
		p.setProperty(FileConfig.SERDE_TYPE, new Integer(SerializerType.NONE.ofType()).toString());
		
		FileSource fileSource = FileSource.newSource(0, p);
		LogicalOperator processor = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator snk = queryAPI.newStatelessSink(new Snk(), 2);
		
		fileSource.connectTo(processor, schema, 0);
		processor.connectTo(snk, 0, new DataStore(schema, DataStoreType.NETWORK));
		
		return queryAPI.build();
	}

}