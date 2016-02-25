import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;


public class Snk implements Sink {
	// time control variables
	long init = 0;
	int sec = 0;
	int events = 0;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}
	long totalDuration =0;
	@Override
	public void processData(ITuple data, API api) {
		int streamId = data.getStreamId();
		
		long timestamp = data.getLong("timestamp");
		long processTime  = data.getInt("processTime");
		System.out.println("streamID: "+streamId+" TS:"+ (System.currentTimeMillis()-timestamp) + " processTime: "+ processTime);
		
		totalDuration += (System.currentTimeMillis() - timestamp);
		events++;
		// TIME CONTROL
		
//		if ((System.currentTimeMillis() - init) > 1000) {
//			System.out.println("[Sink] events/sec:" + sec + " " + c + " ");
//			events = 0;
//			sec++;
//			init = System.currentTimeMillis();
//		}
		
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
