package uk.ac.imperial.lsds.seepcontrib.hdfs.comm;

import java.util.List;
import java.util.Properties;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.MR;
import uk.ac.imperial.lsds.seep.api.Operator;
import uk.ac.imperial.lsds.seep.api.QueryBuilder;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.state.SeepState;

public class SeepReducer implements Operator,MR {
	private static LogicalOperator lo;
	
	private SeepReducer(ReducerTask task,int opId,Schema schema){
		QueryBuilder qb = new QueryBuilder();		
		lo = qb.newStatelessOperator(task, opId);
	}
	public static SeepReducer newSeepReducer(ReducerTask task,int opId,Schema schema){
		return new SeepReducer(task,opId,schema);
	}
	
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema) {
		lo.connectTo(downstreamOperator, streamId, schema);
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, ConnectionType connectionType) {
		// TODO Auto-generated method stub

	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, DataStore dSrc) {
		// TODO Auto-generated method stub

	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			Schema schema, ConnectionType connectionType, DataStore dSrc) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getOperatorId() {
		// TODO Auto-generated method stub
		return lo.getOperatorId();
	}

	@Override
	public String getOperatorName() {
		// TODO Auto-generated method stub
		return lo.getOperatorName();
	}

	@Override
	public boolean isStateful() {
		// TODO Auto-generated method stub
		return lo.isStateful();
	}

	@Override
	public SeepState getState() {
		// TODO Auto-generated method stub
		return lo.getState();
	}

	@Override
	public SeepTask getSeepTask() {
		// TODO Auto-generated method stub
		return lo.getSeepTask();
	}

	@Override
	public List<DownstreamConnection> downstreamConnections() {
		// TODO Auto-generated method stub
		return lo.downstreamConnections();
	}

	@Override
	public List<UpstreamConnection> upstreamConnections() {
		// TODO Auto-generated method stub
		return lo.upstreamConnections();
	}
	@Override
	public LogicalOperator returnlO() {
		// TODO Auto-generated method stub
		return lo;
	}

}
