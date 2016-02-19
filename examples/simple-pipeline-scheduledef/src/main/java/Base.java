import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.DataReference;
import uk.ac.imperial.lsds.seep.api.ScheduleComposer;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.scheduler.StageType;
import uk.ac.imperial.lsds.seep.util.Utils;


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
        InetAddress ip = null;
		try {
			ip = InetAddress.getByName("127.0.0.1");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        int idl1 = Utils.computeIdFromIpAndPort(ip, 3501);
        ControlEndPoint location = new ControlEndPoint(idl1, "127.0.0.1", 3501);
        Stage source = schedAPI.createStage(2, src.getOperatorId(), StageType.SOURCE_STAGE, location);
        Set<DataReference> dRefs = new HashSet<>();
        DataReference dr = DataReference.makeEmptyDataReference(location);
        dRefs.add(dr);
        source.addInputDataReference(0, dRefs);
        
        int idl2 = Utils.computeIdFromIpAndPort(ip, 3502);
        ControlEndPoint location2 = new ControlEndPoint(idl2, "127.0.0.1", 3502);
        Stage intermediate = schedAPI.createStage(1, p.getOperatorId(), StageType.INTERMEDIATE_STAGE, location2);
        Set<DataReference> dRefs2 = new HashSet<>();
        DataReference dr2 = DataReference.makeEmptyDataReference(location2);
        dRefs2.add(dr2);
        intermediate.addInputDataReference(0, dRefs2);
        
        ControlEndPoint location3 = new ControlEndPoint(idl1, "127.0.0.1", 3501);
        Stage sink = schedAPI.createStage(0, snk.getOperatorId(), StageType.SINK_STAGE, location3);
        sink.addInputDataReference(0, dRefs);
        
        schedAPI.declareStages(source, intermediate, sink);
        
        // Create the schedule by chaining the stages
        sink.dependsOn(intermediate);
        intermediate.dependsOn(source);
        
        return schedAPI.build();
	}
}
