import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.ScheduleComposer;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;


public class Base implements ScheduleComposer {

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
        EndPoint location = null;
        Stage source = schedAPI.createStage(2, src.getOperatorId(), StageType.SOURCE_STAGE, location);
        Set<DataReference> dRefs = new HashSet<>();
        DataReference dr = DataReference.makeEmptyDataReference(location);
        dRefs.add(dr);
        source.addInputDataReference(0, dRefs);
        
        EndPoint location2 = null;
        Stage intermediate = schedAPI.createStage(1, p.getOperatorId(), StageType.INTERMEDIATE_STAGE, location2);
        
        EndPoint location3 = null;
        Stage sink = schedAPI.createStage(0, snk.getOperatorId(), StageType.SINK_STAGE, location3);
        
        schedAPI.declareStages(source, intermediate, sink);
        
        // Create the schedule by chaining the stages
        sink.dependsOn(intermediate);
        intermediate.dependsOn(source);
        
        return schedAPI.build();
	}
}
