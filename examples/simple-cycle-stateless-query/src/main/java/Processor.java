import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;


public class Processor implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "ts").newField(Type.STRING, "text").build();
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		long ts = data.getLong("ts");
		
		int streamId = data.getStreamId();
		if(streamId == 1){
			System.out.println("Received from 1, send to 1");
			// from processor2
			api.sendToStreamId(1, data.getData());
			
		} else if(streamId == 0){
			// from source
			if(ts%2 == 0){
				System.out.println("Received from 0, send to 1");
				// send to sink even numbers
				api.sendToStreamId(1, data.getData());
			} else{
				System.out.println("Received from 1, send to 0");
				// send to processor2 odd numbers
				api.sendToStreamId(0, data.getData());
			}
		}
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