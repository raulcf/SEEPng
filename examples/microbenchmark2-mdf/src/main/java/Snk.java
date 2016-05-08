import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;

public class Snk implements Sink {

	private int totalCalls = 0;
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}
	
	boolean first = true;
	int idx_userid = 0;
	int idx_value = 0;
	
	Type[] types = new Type[]{Type.INT, Type.LONG};

	@Override
	public void processData(ITuple data, API api) {
		// setup method not included in scheduled mode
		if(first) {
			first = false;
			idx_userid = data.getIndexFor("userId");
			idx_value = data.getIndexFor("value");
		}
		
		totalCalls++;
		int streamId = data.getStreamId();
		
//		long value = data.getLong("value");
		long value = data.getLong(idx_value);
		
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
