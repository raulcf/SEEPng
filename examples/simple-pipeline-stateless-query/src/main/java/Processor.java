import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Processor implements SeepTask {

	private Schema schema1 = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").newField(Type.STRING, "text").build();
	private Schema schema2 = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		int userId = data.getInt("userId");
		long ts = data.getLong("ts");
		String text = data.getString("text");
		text = text + "_processed";
		
		userId = userId + userId;
		ts = ts - 1;
		
		byte[] processedData = OTuple.create(schema1, new String[]{"userId", "ts", "text"},  new Object[]{userId, ts, text});
//		byte[] processedData = OTuple.create(schema2, new String[]{"userId", "ts"},  new Object[]{userId, ts});
		api.send(processedData);
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
}