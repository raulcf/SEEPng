import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;


public class Branch1 implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
	
	private int branchId;
	
	private int totalCalls = 0;
	
	public Branch1() { }
	
	public Branch1(int branchId) {
		this.branchId = branchId;
	}
	
	boolean first = true;
	int idx_userid = 0;
	int idx_value = 0;
	
	Type[] types = new Type[]{Type.INT, Type.LONG};
	
	OTuple o = new OTuple(schema);
	
	@Override
	public void processData(ITuple data, API api) {
		// setup method not included in scheduled mode
		if(first) {
			first = false;
			idx_userid = data.getIndexFor("userId");
			idx_value = data.getIndexFor("value");
		}
		
		totalCalls++;
		int userId = data.getInt(idx_userid);
		long value = data.getLong(idx_value);
		
		
		value = value / value;

		o.setValues(new Object[]{userId, value});
//		o.setValues(new Object[]{1, 1L});
		api.send(o);
	}

	@Override
	public void setUp() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void close() {
		System.out.println(this + " - TC: " + totalCalls);
	}

	@Override
	public void processDataGroup(List<ITuple> arg0, API arg1) {
		// TODO Auto-generated method stub
		
	}

}
