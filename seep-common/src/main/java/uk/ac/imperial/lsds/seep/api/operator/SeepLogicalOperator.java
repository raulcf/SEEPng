package uk.ac.imperial.lsds.seep.api.operator;

import java.util.ArrayList;
import java.util.List;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.SeepTask;
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

	public void connectTo(Operator downstreamOperator, int streamId, DataStore dataStore) {
		this.connectTo(downstreamOperator, streamId, dataStore, ConnectionType.ONE_AT_A_TIME);
	}
	
	public void connectTo(Operator downstreamOperator, int streamId, DataStore dataStore, ConnectionType connectionType){
		// Add downstream to this operator
		this.addDownstream(downstreamOperator, streamId, dataStore, connectionType);
		// Add this, as upstream, to the downstream operator
		((SeepLogicalOperator)downstreamOperator).addUpstream(this, streamId, dataStore, connectionType);
	}
	
	/* Methods to manage logicalOperator connections */
	
	private void addDownstream(Operator lo, int streamId, DataStore dataStore, ConnectionType connectionType) {
		DownstreamConnection dc = new DownstreamConnection(lo, streamId, dataStore, connectionType);
		this.downstream.add(dc);
	}
	
	private void addUpstream(Operator lo, int streamId, DataStore dataStore, ConnectionType connectionType) {
		UpstreamConnection uc = new UpstreamConnection(lo, streamId, dataStore, connectionType);
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
							+ " with connType: "+up.getConnectionType()+" and dataOrigin: "+up.getDataStoreType());
			sb.append(Utils.NL);
		}
		return sb.toString();
	}

}
