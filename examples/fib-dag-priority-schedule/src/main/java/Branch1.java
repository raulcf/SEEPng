import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Branch1 implements SeepTask {

	Schema schema = SchemaBuilder.getInstance().newField(Type.LONG, "processTime").newField(Type.LONG, "timestamp").build();
	
	@Override
	public void processData(ITuple data, API api) {
		long processTime = data.getLong("processTime");
		long timestamp = data.getLong("timestamp");
		
//		System.out.println("[Brach 1] => TS: " + (System.currentTimeMillis()-timestamp) +" Processing: "+ processTime);
		byte[] processedData = OTuple.create(schema, new String[]{"processTime", "timestamp"},  new Object[]{processTime, timestamp});
		api.send(processedData);
	}

	@Override
	public void setUp() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}

}
