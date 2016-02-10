import java.util.List;

import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.sinks.MarkerSink;
import uk.ac.imperial.lsds.seep.api.operator.sources.SyntheticSource;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DataStoreType;
import uk.ac.imperial.lsds.seep.api.QueryComposer;
import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.Type;


public class Base implements QueryComposer {

	@Override
	public SeepLogicalQuery compose() {
		
		Schema schema = SchemaBuilder.getInstance().newField(Type.INT, "userId").newField(Type.LONG, "value").build();
		
		SyntheticSource synSrc = SyntheticSource.newSource(0, null);
		LogicalOperator map = queryAPI.newStatelessOperator(new Map(), 1);
		
		SeepState ss = null;
		LogicalOperator reduce = queryAPI.newStatefulOperator(new Reduce(), null, 2);
		MarkerSink snk = MarkerSink.newSink(3);
		
		synSrc.connectTo(map, schema, 0);
		map.connectTo(reduce, 2, new DataStore(schema, DataStoreType.NETWORK));
		reduce.connectTo(snk, 5, new DataStore(schema, DataStoreType.NETWORK));
		
		SeepLogicalQuery slq = queryAPI.build();
		slq.setExecutionModeHint(QueryExecutionMode.ALL_SCHEDULED);
		
		return slq;
	}
	
	
	
	class Map implements SeepTask {

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void processData(ITuple arg0, API arg1) {
			// TODO Auto-generated method stub
			
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
	
	class Reduce implements SeepTask {

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void processData(ITuple arg0, API arg1) {
			// TODO Auto-generated method stub
			
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

}
