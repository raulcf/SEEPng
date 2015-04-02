import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Source implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").build();
	private boolean working = true;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		int userId = 0;
		long ts = 0;
		waitHere(2000);
		while(working){
			byte[] d = OTuple.create(schema, new String[]{"userId", "ts"}, new Object[]{userId, ts});
			
			/**
			 * sendKey receives d:byte[] userId that works as the partitioning key in this case 
			 * Note that normally, the partitioning key will have some relation with the downstream state (although this is 
			 * application specific)
			 */
			api.sendKey(d, userId);
			
			userId++;
			ts++;
		}
	}
	
	private void waitHere(int time){
		try {
			Thread.sleep(time);
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		this.working = false;
	}
}
