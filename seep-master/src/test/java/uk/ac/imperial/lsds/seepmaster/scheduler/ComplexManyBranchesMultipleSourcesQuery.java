package uk.ac.imperial.lsds.seepmaster.scheduler;

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

public class ComplexManyBranchesMultipleSourcesQuery implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		// Declare Source
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), -1);
		LogicalOperator src2 = queryAPI.newStatelessSource(new Source(), -2);
		LogicalOperator src3 = queryAPI.newStatelessSource(new Source(), -3);
		// Declare processors
		LogicalOperator p1 = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator p2 = queryAPI.newStatelessOperator(new Processor(), 2);
		LogicalOperator p3 = queryAPI.newStatelessOperator(new Processor(), 3);
		LogicalOperator p4 = queryAPI.newStatelessOperator(new Processor(), 4);
		LogicalOperator p5 = queryAPI.newStatelessOperator(new Processor(), 5);
		LogicalOperator p6 = queryAPI.newStatelessOperator(new Processor(), 6);
		LogicalOperator p7 = queryAPI.newStatelessOperator(new Processor(), 7);
		LogicalOperator p8 = queryAPI.newStatelessOperator(new Processor(), 8);
		LogicalOperator p9 = queryAPI.newStatelessOperator(new Processor(), 9);
		LogicalOperator p10 = queryAPI.newStatelessOperator(new Processor(), 10);
		LogicalOperator p11 = queryAPI.newStatelessOperator(new Processor(), 11);
		LogicalOperator p12 = queryAPI.newStatelessOperator(new Processor(), 12);
		LogicalOperator p13 = queryAPI.newStatelessOperator(new Processor(), 13);
		LogicalOperator p14 = queryAPI.newStatelessOperator(new Processor(), 14);
		LogicalOperator p15 = queryAPI.newStatelessOperator(new Processor(), 15);
		LogicalOperator p16 = queryAPI.newStatelessOperator(new Processor(), 16);
		LogicalOperator p17 = queryAPI.newStatelessOperator(new Processor(), 17);
		LogicalOperator p18 = queryAPI.newStatelessOperator(new Processor(), 18);
		LogicalOperator p19 = queryAPI.newStatelessOperator(new Processor(), 19);
		// Declare sink
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), 100);
		
		Schema srcSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").build();
		
		/** Connect operators **/ // Note streamId is totally wrong here
		src.connectTo(p13, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		src3.connectTo(p2, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		src2.connectTo(p1, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p1.connectTo(p2, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p1.connectTo(p3, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		src2.connectTo(p6, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		src2.connectTo(p12, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p2.connectTo(p4, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p3.connectTo(p4, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p4.connectTo(p5, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p6.connectTo(p7, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p7.connectTo(p8, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p8.connectTo(p9, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p9.connectTo(p10, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p5.connectTo(p10, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p10.connectTo(p11, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p12.connectTo(p13, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p13.connectTo(p14, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p14.connectTo(p15, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p15.connectTo(p18, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p15.connectTo(p16, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p16.connectTo(p17, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p18.connectTo(p19, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p11.connectTo(p19, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p17.connectTo(snk, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		p19.connectTo(snk, 0, new DataStore(srcSchema, DataStoreType.NETWORK, null));
		
		
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
		public void processDataGroup(ITuple dataList, API api) {
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
		public void processDataGroup(ITuple dataList, API api) {
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
		public void processDataGroup(ITuple dataList, API api) {
			// TODO Auto-generated method stub	
		}
		@Override
		public void close() {
			// TODO Auto-generated method stub	
		}
	}
	
}
