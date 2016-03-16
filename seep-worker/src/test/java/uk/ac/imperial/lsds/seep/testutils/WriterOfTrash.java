package uk.ac.imperial.lsds.seep.testutils;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;

public class WriterOfTrash implements SeepTask {

	private Schema schema;
	
	public WriterOfTrash(Schema schema) {
		this.schema = schema;
	}
	
	@Override
	public void setUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processData(ITuple data, API api) {
		
		// just write fake data
		byte[] o = OTuple.create(schema, new String[]{"4bytes"}, new Object[]{4});
		api.send(o);
		
	}

	@Override
	public void processDataGroup(List<ITuple> d, API api) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
