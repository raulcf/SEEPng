import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;

public class Adder implements SeepTask {

	private Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
	private Double selectivity = 0;
	int processed = 0, sent = 0;
	
	public Adder(Double sel) {
		selectivity = sel;
	}
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
	}

	@Override
	public void processData(ITuple data, API api) {
		int userId = data.getInt("userId");
		long value = data.getLong("value");
		processed++;
		
		value++;
		
		while (((double)sent/(double)processed) < selectivity) {
			byte[] processedData = OTuple.create(schema, new String[]{"userId", "value"},  new Object[]{userId, value});
			api.send(processedData);
			sent++;
		}
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