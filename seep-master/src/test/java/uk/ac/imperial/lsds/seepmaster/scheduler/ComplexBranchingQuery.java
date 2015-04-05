package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;

public class ComplexBranchingQuery implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		// Declare Source
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), -1);
		// Declare processors
		LogicalOperator p = queryAPI.newStatelessOperator(new Processor(), 1);
		LogicalOperator p2 = queryAPI.newStatelessOperator(new Processor(), 2);
		LogicalOperator p3 = queryAPI.newStatelessOperator(new Processor(), 3);
		LogicalOperator p4 = queryAPI.newStatelessOperator(new Processor(), 4);
		LogicalOperator p5 = queryAPI.newStatelessOperator(new Processor(), 5);
		// Declare sink
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), -2);
		
		Schema srcSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").build();
		
		/** Connect operators **/
		src.connectTo(p, 0, srcSchema);
		src.connectTo(p3, 0, srcSchema);
		p.connectTo(p2, 0, srcSchema);
		p3.connectTo(p4, 0, srcSchema);
		p4.connectTo(p5, 0, srcSchema);
		p5.connectTo(snk, 0, srcSchema);
		p2.connectTo(snk, 0, srcSchema);
		
		return QueryBuilder.build();
	}

	
	class Source implements uk.ac.imperial.lsds.seep.api.sources.Source {
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
	
	class Sink implements uk.ac.imperial.lsds.seep.api.sinks.Sink {
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
