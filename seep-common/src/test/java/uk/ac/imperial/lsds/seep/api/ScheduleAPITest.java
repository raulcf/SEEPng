package uk.ac.imperial.lsds.seep.api;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
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
		
		// Then add the operators in stages
		ControlEndPoint location = null;
		Stage source = schedAPI.createStage(0, src.getOperatorId(), StageType.SOURCE_STAGE, location);
		
		ControlEndPoint location2 = null;
		Stage intermediate = schedAPI.createStage(1, p.getOperatorId(), StageType.INTERMEDIATE_STAGE, location2);
		
		ControlEndPoint location3 = null;
		Stage sink = schedAPI.createStage(2, snk.getOperatorId(), StageType.SINK_STAGE, location3);
		
		// Create the schedule by chaining the stages
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
