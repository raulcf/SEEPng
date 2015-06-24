import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;


public class Src implements Source {

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
		while(working){
			byte[] d = OTuple.create(schema, new String[]{"userId", "ts", "text"}, new Object[]{userId, ts, "!akfaK morf olleH"});
			api.send(d);
			
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
	public void close() {
		this.working = false;
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}
}
