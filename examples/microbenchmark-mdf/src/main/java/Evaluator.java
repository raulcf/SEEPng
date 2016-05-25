import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;



public class Evaluator implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
	private int totalCalls = 0;
	
	@Override
	public void close() {
		System.out.println(this + " - TC: " + totalCalls);
	}

	boolean first = true;
	int idx_userid = 0;
	int idx_value = 0;
	Type[] types = new Type[]{Type.INT, Type.LONG};

	OTuple o = new OTuple(schema);
	
	@Override
	public void processData(ITuple data, API arg1) {
		// setup method not included in scheduled mode
		if(first) {
			first = false;
			idx_userid = data.getIndexFor("userId");
			idx_value = data.getIndexFor("value");
		}
		
		totalCalls++;
		int userId = data.getInt(idx_userid);
		long value = data.getLong(idx_value);
		// do some calculation with the utility function 
		arg1.storeEvaluateResults(System.currentTimeMillis()); // a long, as an abstract notion of quality
		// propagate the results downstream
		

		o.setValues(new Object[]{userId, value});
//		o.setValues(new Object[]{1, 1L});
		arg1.send(o);
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
