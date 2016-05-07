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
	
	@Override
	public void processData(ITuple data, API api) {
		totalCalls++;
		int userId = data.getInt("userId");
		long value = data.getLong("value");
		
//		System.out.println("bid: " + branchId);
		
		value = value / value;
		
		byte[] processedData = OTuple.create(schema, new String[]{"userId", "value"},  new Object[]{userId, value});
		api.send(processedData);
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
