import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Map implements SeepTask {

	Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processData(ITuple arg0, API api) {
		int userId = arg0.getInt("userId");
		long value = arg0.getLong("value");
		
		userId++;
		value++;
		
		byte[] ot = OTuple.create(schema, schema.names(), new Object[]{userId, value});
		System.out.println("UID: " + userId);
		api.sendKey(ot, userId);
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setUp() {
		// TODO Auto-generated method stub
		
	}
	
}