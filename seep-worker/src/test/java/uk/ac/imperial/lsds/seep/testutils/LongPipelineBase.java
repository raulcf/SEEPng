package uk.ac.imperial.lsds.seep.testutils;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public class LongPipelineBase implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		// Declare Source
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), -1);
		LogicalOperator src2 = queryAPI.newStatelessSource(new Source(), -10);
		
		// Declare processors
		LogicalOperator p = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator p2 = queryAPI.newStatelessOperator(new Processor(), 2);
		LogicalOperator p3 = queryAPI.newStatelessOperator(new Processor(), 3);
		LogicalOperator p4 = queryAPI.newStatelessOperator(new Processor(), 4);
		LogicalOperator p5 = queryAPI.newStatelessOperator(new Processor(), 5);
		// Declare sink
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), -2);
				
		Schema srcSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").build();
		//Schema pSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").newField(Type.BYTES, "payload").build();
				
		/** Connect operators **/
		src.connectTo(p, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		src2.connectTo(p, 1, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		
		p.connectTo(p2, 2, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p2.connectTo(p3, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		
		p3.connectTo(p4, 2, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p3.connectTo(p5, 1, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		
		p4.connectTo(snk, 3, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p5.connectTo(snk, 4, new DataStore(srcSchema, DataStoreType.NETWORK, null));
				
		return QueryBuilder.build();
	}
	
	class Source implements uk.ac.imperial.lsds.seep.api.operator.sources.Source {
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(List<ITuple> dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
	
	class Processor implements SeepTask {
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(List<ITuple> dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
	
	class Sink implements uk.ac.imperial.lsds.seep.api.operator.sinks.Sink {
		@Override
		public void setUp() {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processData(ITuple data, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void processDataGroup(List<ITuple> dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}

}
