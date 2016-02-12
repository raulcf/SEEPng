import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class Reduce implements SeepTask {

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processData(ITuple arg0, API arg1) {
		int userId = arg0.getInt("userId");
		long value = arg0.getLong("value");
		
		System.out.println("uid: " + userId);
		
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