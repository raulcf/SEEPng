import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;


public class Sink implements SeepTask {

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
	public void processDataGroup(ITuple dataBatch, API api) {
		System.out.println("In barrier: ");
		while(dataBatch.hasNext()){
			ITuple data = dataBatch.next();
			int userId = data.getInt("userId");
			long ts = data.getLong("ts");
			String text = data.getString("text");
			
			System.out.println("UID: "+userId+" ts: "+ts+" text: "+text);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

}
