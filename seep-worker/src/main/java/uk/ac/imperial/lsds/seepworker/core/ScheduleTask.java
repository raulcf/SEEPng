package uk.ac.imperial.lsds.seepworker.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.API;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.data.ITuple;
import uk.ac.imperial.lsds.seep.api.data.OTuple;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.data.TransporterITuple;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.scheduler.ScheduleDescription;
import uk.ac.imperial.lsds.seep.scheduler.Stage;
import uk.ac.imperial.lsds.seep.util.Utils;

public class ScheduleTask implements SeepTask {

	final private static Logger LOG = LoggerFactory.getLogger(ScheduleTask.class.getName());
	
	private int stageId;
	private int euId;
	private List<LogicalOperator> operators;
	private Iterator<LogicalOperator> opIt;
	private List<SeepTask> tasks;
	private Iterator<SeepTask> taskIterator;
	private API scApi = new SimpleCollector();
	
	// Optimizing for same schema
	private boolean sameSchema = true;
	private Schema schema = null;
	private ITuple data = null;
	private TransporterITuple d = null;
	
	private ScheduleTask(int euId, int stageId, List<LogicalOperator> operators) {
		this.stageId = stageId;
		this.euId = euId;
		this.operators = operators;
		this.tasks = new ArrayList<>();
		this.opIt = operators.iterator();
		while(opIt.hasNext()) {
			tasks.add(opIt.next().getSeepTask());
		}
		this.taskIterator = tasks.iterator();
		// TODO: initialize sameSchema here by actually checking if the schema is the same
		if(sameSchema && opIt.hasNext()) {
			schema = opIt.next().downstreamConnections().get(0).getSchema();
			data = new ITuple(schema);
			d = new TransporterITuple(schema);
			opIt = operators.iterator(); // reset
		}
	}
	
	public static ScheduleTask buildTaskFor(int id, Stage s, ScheduleDescription sd) {
		Deque<Integer> wrappedOps = s.getWrappedOperators();
		LOG.info("Building stage {}. Wraps {} operators", s.getStageId(), wrappedOps.size());
//		Deque<LogicalOperator> operators = new ArrayDeque<>();
		List<LogicalOperator> operators = new ArrayList<>();
		while(! wrappedOps.isEmpty()) {
			LogicalOperator lo = sd.getOperatorWithId(wrappedOps.poll());
			LOG.debug("op {} is part of stage {}", lo.getOperatorId(), s.getStageId());
			operators.add(lo);
		}
		return new ScheduleTask(id, s.getStageId(), operators);
	}
	
	public int getStageId() {
		return stageId;
	}
	
	public int getEuId() {
		return euId;
	}
	
	@Override
	public void setUp() {
		if(! taskIterator.hasNext()) {
			taskIterator = tasks.iterator();
		}
		if(! opIt.hasNext()) {
			opIt = operators.iterator();
		}
	}

//	@Override
	@Deprecated
	public void _processData(ITuple data, API api) {
		API scApi = new SimpleCollector();
		SeepTask next = taskIterator.next(); // Get first, and possibly only task here
		// Check whether there are tasks ahead
		while(taskIterator.hasNext()) {
			// There is a next OP, we simply need to collect output
			next.processData(data, scApi);
			byte[] o = ((SimpleCollector)scApi).collectMem();
			LogicalOperator nextOp = opIt.next();
			if(! sameSchema) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
				data = new ITuple(schema);
			}
			data.setData(o);
			// Otherwise we simply forward the data
			next = taskIterator.next();
		}
		// Finally use real API for real forwarding
		next.processData(data, api);
		// Then reset iterators for more processing
		taskIterator = tasks.iterator();
		opIt = operators.iterator();
	}
	
	@Override
	public void processData(ITuple data, API api) {
		OTuple o = null;
		Schema lSchema = null;
		boolean taskProducedEmptResult = false;
		
		for(int i = 0; i < tasks.size() - 1; i++) {
			((SimpleCollector)scApi).reset();
			SeepTask next = tasks.get(i);
			next.processData(data, scApi);
			o = ((SimpleCollector)scApi).collect();
			
			if(o == null || o.getData() == null) {
				taskProducedEmptResult = true;
				break;
			}
			LogicalOperator nextOp = operators.get(i);
			if(! sameSchema) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
				data = new ITuple(schema);
				d = new TransporterITuple(schema); // FIXME: can we get schema from OTuple
			}
			if(d == null) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema();
				d = new TransporterITuple(schema);
			}
			Object[] values = o.getValues();
			d.setValues(values);
		}
		
		if (!taskProducedEmptResult) {
			SeepTask next = tasks.get(tasks.size() -1);
			next.processData(data, api);
		}
		
//		SeepTask next = taskIterator.next(); // Get first, and possibly only task here
//		// Check whether there are tasks ahead
//		while(taskIterator.hasNext()) {
//			// There is a next OP, we simply need to collect output
//			next.processData(data, scApi);
////			byte[] o = ((SimpleCollector)scApi).collect();
//			OTuple o = ((SimpleCollector)scApi).collect();
//			LogicalOperator nextOp = opIt.next();
//			if(! sameSchema) {
//				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
//				data = new ITuple(schema);
//				d = new TransporterITuple(schema); // FIXME: can we get schema from OTuple
//			}
//			if(d == null) {
//				Schema schema = nextOp.downstreamConnections().get(0).getSchema();
//				d = new TransporterITuple(schema);
//			}
//			Object[] values = o.getValues();
//			d.setValues(values);
//			// Otherwise we simply forward the data
//			next = taskIterator.next();
//		}
//		
//		// Finally use real API for real forwarding
//		next.processData(data, api);
//		// Then reset iterators for more processing
//		taskIterator = tasks.iterator();
//		opIt = operators.iterator();
	}
	
	
	public void __processData(ITuple data, API api) {
		SeepTask next = taskIterator.next(); // Get first, and possibly only task here
		// Check whether there are tasks ahead
		while(taskIterator.hasNext()) {
			// There is a next OP, we simply need to collect output
			next.processData(data, scApi);
//			byte[] o = ((SimpleCollector)scApi).collect();
			OTuple o = ((SimpleCollector)scApi).collect();
			LogicalOperator nextOp = opIt.next();
			if(! sameSchema) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema(); // 0 cause there's only 1
				data = new ITuple(schema);
				d = new TransporterITuple(schema); // FIXME: can we get schema from OTuple
			}
			if(d == null) {
				Schema schema = nextOp.downstreamConnections().get(0).getSchema();
				d = new TransporterITuple(schema);
			}
			Object[] values = o.getValues();
			d.setValues(values);
			// Otherwise we simply forward the data
			next = taskIterator.next();
		}
		
		// Finally use real API for real forwarding
		next.processData(data, api);
		// Then reset iterators for more processing
		taskIterator = tasks.iterator();
		opIt = operators.iterator();
	}
	
	public boolean hasMoreTasks() {
		return taskIterator.hasNext();
	}

	@Override
	public void processDataGroup(List<ITuple> dataBatch, API api) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		if(taskIterator.hasNext()){
			taskIterator.next().close();
		}
		else{
			taskIterator = tasks.iterator();
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Stage: " + this.stageId + ", running on: " + this.euId);
		sb.append(Utils.NL);
		StringBuffer tasksDescr = new StringBuffer();
		for(LogicalOperator lo : operators) {
			tasksDescr.append(" t: " + lo.getOperatorId() + "-> " + lo.getSeepTask().toString());
			tasksDescr.append(Utils.NL);
		}
		sb.append(tasksDescr.toString());
		return sb.toString();
	}
}
