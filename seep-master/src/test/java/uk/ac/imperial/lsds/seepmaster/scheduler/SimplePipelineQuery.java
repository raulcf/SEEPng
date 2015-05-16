package uk.ac.imperial.lsds.seepmaster.scheduler;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;

public class SimplePipelineQuery implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		// Declare Source
		LogicalOperator src = queryAPI.newStatelessSource(new Source(), -1);
		// Declare processor
		LogicalOperator p = queryAPI.newStatelessOperator(new Processor(), 1);
		// Declare sink
		LogicalOperator snk = queryAPI.newStatelessSink(new Sink(), -2);
		
		Schema srcSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").build();
		Schema pSchema = queryAPI.schemaBuilder.newField(Type.SHORT, "id").newField(Type.BYTES, "payload").build();
		
		/** Connect operators **/
		src.connectTo(p, 0, srcSchema);
		p.connectTo(snk, 0, pSchema);
		
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
