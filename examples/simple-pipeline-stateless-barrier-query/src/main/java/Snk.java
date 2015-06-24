import java.util.Iterator;
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
		int userId = data.getInt("userId");
		long ts = data.getLong("ts");
		String text = data.getString("text");
		
		System.out.println("UID: "+userId+" ts: "+ts+" text: "+text);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		System.out.println("In barrier: ");
		Iterator<ITuple> it = arg0.iterator();
		while(it.hasNext()){
			ITuple data = it.next();
			int userId = data.getInt("userId");
			long ts = data.getLong("ts");
			String text = data.getString("text");
			
			System.out.println("UID: "+userId+" ts: "+ts+" text: "+text);
		}
		
	}

}
