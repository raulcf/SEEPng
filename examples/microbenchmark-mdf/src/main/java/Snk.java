import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;

public class Snk implements Sink {

	private int totalCalls = 0;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		totalCalls++;
		int streamId = data.getStreamId();
		
		long value = data.getLong("value");
		
//		System.out.println("streamID: "+streamId+" value: "+value);
	}

	@Override
	public void close() {
		System.out.println(this + " - TC: " + totalCalls);
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		//non implemented
	}

}
