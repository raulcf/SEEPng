import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class Sink implements SeepTask {

	@Override
	public void processData(ITuple data, API api) {
		
		System.out.println("data size: "+data.getData().length);
		
		int param1 = data.getInt("param1");
		int param2 = data.getInt("param2");
		
		System.out.println("P1: "+param1+" P2: "+param2);
	}

	@Override
	public void processDataGroup(ITuple dataBatch, API api) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

}
