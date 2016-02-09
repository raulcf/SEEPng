package uk.ac.imperial.lsds.seep.api;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;

public class ScheduleAPITest implements ScheduleComposer {

	@Override
	public ScheduleDescription compose() {
		
		// First declare the general operators we need
		
		// Declare Source
		LogicalOperator src = schedAPI.newStatelessSource(new CustomSource(), 0);
		// Declare processor
		LogicalOperator p = schedAPI.newStatelessOperator(new Processor(), 1);
		// Declare sink
		LogicalOperator snk = schedAPI.newStatelessSink(new CustomSink(), 2);
		
		// Then put those operators as part of the schedule
		
		Stage source = new Stage(0);
		source.add(src.getOperatorId());
		source.setStageType(StageType.SOURCE_STAGE);
		Schema schema = null; // declare here what is the schema of the data
		Properties config = new Properties();
		DataStore dataStore = new DataStore(schema, DataStoreType.CUSTOM_SYNTHETIC, config);
		DataReference dRef = DataReference.makeExternalDataReference(dataStore);
		Set<DataReference> drefs = new HashSet<>();
		drefs.add(dRef);
		source.addInputDataReference(0, drefs);
		
		Stage intermediate = new Stage(1);
		intermediate.add(p.getOperatorId());
		intermediate.setStageType(StageType.INTERMEDIATE_STAGE);
		
		Stage sink = new Stage(2);
		sink.add(snk.getOperatorId());
		sink.setStageType(StageType.SINK_STAGE);
		
		sink.dependsOn(intermediate);
		intermediate.dependsOn(source);
		
		return schedAPI.build();
	}
	
	class CustomSource implements uk.ac.imperial.lsds.seep.api.operator.sources.Source {
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
	
	class Processor implements SeepTask{
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
	
	class CustomSink implements uk.ac.imperial.lsds.seep.api.operator.sinks.Sink {
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
