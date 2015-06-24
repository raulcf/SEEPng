import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;


public class Snk implements Sink {
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		String text = data.getString("text");
		int userId = data.getInt("userId");
		long ts = data.getLong("ts");
		
		System.out.println("[Sink] text: " + text + " user: " + userId + " ts: " + ts);
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
