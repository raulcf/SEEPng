package uk.ac.imperial.lsds.seep.api;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.util.Utils;

public class SeepLogicalOperator implements LogicalOperator {

	private int opId;
	private String name;
	private boolean stateful;
	private SeepTask task;
	private SeepState state;
	
	private List<DownstreamConnection> downstream = new ArrayList<DownstreamConnection>();
	private List<UpstreamConnection> upstream = new ArrayList<UpstreamConnection>();
	
	private SeepLogicalOperator(int opId, SeepTask task, String name){
		this.opId = opId;
		this.task = task;
		this.name = name;
		this.stateful = false;
	}
	
	private SeepLogicalOperator(int opId, SeepTask task, SeepState state, String name){
		this.opId = opId;
		this.task = task;
		this.name = name;
		this.state = state;
		this.stateful = true;
	}
	
	public static LogicalOperator newStatelessOperator(int opId, SeepTask task){
		String name = new Integer(opId).toString();
		return SeepLogicalOperator.newStatelessOperator(opId, task, name);
	}
	
	public static LogicalOperator newStatelessOperator(int opId, SeepTask task, String name){
		return new SeepLogicalOperator(opId, task, name);
	}
	
	public static LogicalOperator newStatefulOperator(int opId, SeepTask task, SeepState state){
		String name = new Integer(opId).toString();
		return SeepLogicalOperator.newStatefulOperator(opId, task, state, name);
	}
	
	public static LogicalOperator newStatefulOperator(int opId, SeepTask task, SeepState state, String name){
		return new SeepLogicalOperator(opId, task, state, name);
	}
	
	@Override
	public int getOperatorId() {
		return opId;
	}

	@Override
	public String getOperatorName() {
		return name;
	}

	@Override
	public boolean isStateful() {
		return stateful;
	}
	
	@Override
	public SeepState getState() {
		return state;
	}

	@Override
	public SeepTask getSeepTask() {
		return task;
	}

	@Override
	public List<DownstreamConnection> downstreamConnections() {
		return downstream;
	}

	@Override
	public List<UpstreamConnection> upstreamConnections() {
		return upstream;
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema) {
		this.connectTo(downstreamOperator, streamId, schema, ConnectionType.ONE_AT_A_TIME);
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, DataStore dSrc) {
		this.connectTo(downstreamOperator, streamId, schema, ConnectionType.ONE_AT_A_TIME, dSrc);
	}
	
	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType) {
		DataStore dO = new DataStore(DataStoreType.NETWORK, null);
		this.connectTo(downstreamOperator, streamId, schema, connectionType, dO);
	}
	
	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType, DataStore dSrc){
		// Add downstream to this operator
		this.addDownstream(downstreamOperator, streamId, schema, connectionType, dSrc);
		// Add this, as upstream, to the downstream operator
		((SeepLogicalOperator)downstreamOperator).addUpstream(this, connectionType, streamId, schema, dSrc);
	}
	
	/* Methods to manage logicalOperator connections */
	
	private void addDownstream(Operator lo, int streamId, Schema schema, ConnectionType connectionType, DataStore dSrc){
		DownstreamConnection dc = new DownstreamConnection(lo, streamId, schema, connectionType, dSrc);
		this.downstream.add(dc);
	}
	
	private void addUpstream(Operator lo, ConnectionType connectionType, int streamId, Schema schema, DataStore dSrc){
		UpstreamConnection uc = new UpstreamConnection(lo, connectionType, streamId, schema, dSrc);
		this.upstream.add(uc);
	}
	
	/* Methods to print info about the operator */
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("LogicalOperator");
		sb.append(Utils.NL);
		sb.append("###############");
		sb.append(Utils.NL);
		sb.append("Name: "+this.name);
		sb.append(Utils.NL);
		sb.append("OpId: "+this.opId);
		sb.append(Utils.NL);
		sb.append("Stateful?: "+this.stateful);
		sb.append(Utils.NL);
		sb.append("#Downstream: "+this.downstream.size());
		sb.append(Utils.NL);
		for(int i = 0; i<this.downstream.size(); i++){
			DownstreamConnection down = downstream.get(i);
			sb.append("  Down-conn-"+i+"-> StreamId: "+down.getStreamId()+" to opId: "
					+ ""+down.getDownstreamOperator().getOperatorId());
			sb.append(Utils.NL);
		}
		sb.append("#Upstream: "+this.upstream.size());
		sb.append(Utils.NL);
		for(int i = 0; i<this.upstream.size(); i++){
			UpstreamConnection up = upstream.get(i);
			sb.append("  Up-conn-"+i+"-> StreamId: "+up.getStreamId()+" to opId: "
					+ ""+up.getUpstreamOperator().getOperatorId()+""
							+ " with connType: "+up.getConnectionType()+" and dataOrigin: "+up.getDataOriginType());
			sb.append(Utils.NL);
		}
		return sb.toString();
	}

}
