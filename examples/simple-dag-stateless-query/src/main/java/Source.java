import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Source implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").newField(Type.STRING, "text").build();
	private boolean working = true;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		int userId = 0;
		long ts = 0;
		int switcher = 0;
		while(working){
			byte[] d = OTuple.create(schema, new String[]{"userId", "ts", "text"}, new Object[]{userId, ts, "some text"});
			
			if(switcher++%2 == 0)
				api.sendToStreamId(0, d);
			else
				api.sendToStreamId(1, d);
			
			userId++;
			ts++;
			
			waitHere(1000);
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
