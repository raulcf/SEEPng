import java.util.List;
import java.util.UUID;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;

public class Adder implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
	private Double selectivity = 0.;
	private int processed = 0, sent = 0;
	private String adderId;
	private boolean used;
	private int compfactor;
	
	private int totalCalls = 0;
	
	public Adder() {
		selectivity = 1.;
		adderId = UUID.randomUUID().toString();
		used = false;
	}
	
	public Adder(Double sel, int compfactor) {
		selectivity = sel;
		adderId = UUID.randomUUID().toString();
		used = false;
		this.compfactor = compfactor;
	}
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
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
		
//		int userId = data.getInt("userId");
//		long value = data.getLong("value");
		int userId = data.getInt(idx_userid);
		long value = data.getLong(idx_value);
		processed++;
		
		if (!used) {
			System.out.println(adderId + " has started with selectivity " + selectivity);
			used = true;
		}
				
		for(int i = 0; i< compfactor; i++) {
			value = (long) Math.sqrt((double)(value / 2));
			Math.pow(value, value);
		}
		
		while (((double)sent/(double)processed) < selectivity) {
//			byte[] processedData = OTuple.create(schema, new String[]{"userId", "value"},  new Object[]{userId, value});
//			byte[] processedData = OTuple.createUnsafe(types, new Object[]{userId, value}, 12);
//			api.send(processedData);
			o.setValues(new Object[]{userId, value});
			api.send(o);
			sent++;
		}
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