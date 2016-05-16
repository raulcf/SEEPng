package uk.ac.imperial.lsds.seepworker.core;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.data.TransporterITuple;
import uk.ac.imperial.lsds.seep.api.data.Type;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.testutils.LongPipelineBase;

public class ScheduleTaskTest {

	@Test
	public void testCreateScheduleTasks() {
		LongPipelineBase lpb = new LongPipelineBase();
		SeepLogicalQuery lsq = lpb.compose();
		
		// Create stage manually (scheduler not accessible from this module)
		
		Stage s1 = new Stage(0);
		s1.add(-1);
		Stage s2 = new Stage(1);
		s2.add(-10);
		Stage s3 = new Stage(2);
		s3.add(1);
		s3.add(2);
		s3.add(3);
		Stage s4 = new Stage(3);
		s4.add(4);
		Stage s5 = new Stage(4);
		s5.add(5);
		Stage s6 = new Stage(5);
		s6.add(-2);
		
		// Create ScheduleDescription, with stages and operators
		Set<Stage> stages = new HashSet<>();
		stages.add(s1);
		stages.add(s2);
		stages.add(s3);
		stages.add(s4);
		stages.add(s5);
		stages.add(s6);
		ScheduleDescription sd = new ScheduleDescription(stages, lsq.getAllOperators());
		
		// Build scheduleTasks from Stages
		ScheduleTask st1 = ScheduleTask.buildTaskFor(0, s1, sd);
		ScheduleTask st2 = ScheduleTask.buildTaskFor(0, s2, sd);
		ScheduleTask st3 = ScheduleTask.buildTaskFor(0, s3, sd);
		ScheduleTask st4 = ScheduleTask.buildTaskFor(0, s4, sd);
		ScheduleTask st5 = ScheduleTask.buildTaskFor(0, s5, sd);
		ScheduleTask st6 = ScheduleTask.buildTaskFor(0, s6, sd);

		// setUp tasks
		st1.setUp();
		st2.setUp();
		st3.setUp();
		st4.setUp();
		st5.setUp();
		st6.setUp();
		
		// print tasks for visual inspection
		System.out.println("T1: "+st1.toString());
		System.out.println("T2: "+st2.toString());
		System.out.println("T3: "+st3.toString());
		System.out.println("T4: "+st4.toString());
		System.out.println("T5: "+st5.toString());
		System.out.println("T6: "+st6.toString());
		
		List<Integer> l = new ArrayList<>();
		Iterator<Integer> i = l.iterator();
		boolean yes = i.hasNext();
		
		// run tasks
		Schema schema = SchemaBuilder.getInstance().newField(Type.SHORT, "id").build();
		//byte[] d = OTuple.create(schema, schema.names(), schema.defaultValues());
		OTuple o = new OTuple(schema);
		o.setValues(schema.defaultValues());
		//ITuple data = new ITuple(schema);
		TransporterITuple data = new TransporterITuple(schema);
//		data.setData(d);
		data.setValues(o.getValues());
		API api = new SimpleCollector();
		st1.processData(data, api);
		//byte[] output = ((SimpleCollector)api).collect();
		OTuple ot = ((SimpleCollector)api).collect();
		TransporterITuple out = new TransporterITuple(schema);
		out.setValues(ot.getValues());
		System.out.println(out.toString());
		
		st3.processData(data, api);
		OTuple output = ((SimpleCollector)api).collect();
		out = new TransporterITuple(schema);
		out.setValues(output.getValues());
		System.out.println(out.toString());
				
	}

}
