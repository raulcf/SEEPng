package uk.ac.imperial.lsds.seep.api;

import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.Schema;
import uk.ac.imperial.lsds.seep.api.state.SeepState;
import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;
import uk.ac.imperial.lsds.seep.util.Utils;

public class SeepQueryPhysicalOperator implements PhysicalOperator{
	
	private int opId;
	private String name;
	private SeepTask seepTask;
	private SeepState state;
	private boolean stateful = false;
	private List<DownstreamConnection> downstreamConnections;
	private List<UpstreamConnection> upstreamConnections;
	private final EndPoint ep;
	
	
	private SeepQueryPhysicalOperator(int opId, String name, SeepTask seepTask, 
									SeepState state, List<DownstreamConnection> downstreamConnections, 
									List<UpstreamConnection> upstreamConnections, EndPoint ep) {
		this.opId = opId;
		this.name = name;
		this.seepTask = seepTask;
		this.state = state;
		if(this.state != null){
			this.stateful = true;
		}
		this.downstreamConnections = downstreamConnections;
		this.upstreamConnections = upstreamConnections;
		this.ep = ep;
	}
	
	public static SeepQueryPhysicalOperator createPhysicalOperatorFromLogicalOperatorAndEndPoint(int opId, Operator lo, EndPoint ep){
		return new SeepQueryPhysicalOperator(opId, lo.getOperatorName(), lo.getSeepTask(), 
				lo.getState(), lo.downstreamConnections(), lo.upstreamConnections(), ep);
	}
	
	public static SeepQueryPhysicalOperator createPhysicalOperatorFromLogicalOperatorAndEndPoint(Operator lo, EndPoint ep){
		return SeepQueryPhysicalOperator.createPhysicalOperatorFromLogicalOperatorAndEndPoint(lo.getOperatorId(), lo, ep);
	}
	
	public static SeepQueryPhysicalOperator createPhysicalOperatorFromScratch(int opId, String opName, SeepTask st,
			SeepState state, List<DownstreamConnection> downCons, List<UpstreamConnection> upCons, EndPoint ep){
		return new SeepQueryPhysicalOperator(opId, opName, st, state, downCons, upCons, ep);
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
		return seepTask;
	}

	@Override
	public List<DownstreamConnection> downstreamConnections() {
		return this.downstreamConnections;
	}

	@Override
	public List<UpstreamConnection> upstreamConnections() {
		return this.upstreamConnections;
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema) {
		replaceDownstream(downstreamOperator);
		replaceUpstream(downstreamOperator);
	}
	
	private void replaceDownstream(Operator downstreamOperator) {
		int opIdToUpdate = downstreamOperator.getOperatorId();
		for(DownstreamConnection dc : this.downstreamConnections) {
			if(dc.getDownstreamOperator().getOperatorId() == opIdToUpdate) {
				dc.replaceOperator(downstreamOperator);
			}
		}
	}
	
	private void replaceUpstream(Operator downstreamOperator) {
		for(UpstreamConnection uc : downstreamOperator.upstreamConnections()) {
			if(uc.getUpstreamOperator().getOperatorId() == this.getOperatorId()) {
				uc.replaceOperator(this);
			}
		}
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType) {
		throw new UnsupportedOperationException("Should not use this connectTo in Physical Ops");
	}
	
	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, DataStore dSrc) {
		throw new UnsupportedOperationException("Should not use this connectTo in Physical Ops");
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId, Schema schema, ConnectionType connectionType, DataStore dSrc) {
		throw new UnsupportedOperationException("Should not use this connectTo in Physical Ops");
	}
	
	@Override
	public int getIdOfWrappingExecutionUnit() {
		return this.ep.getId();
	}
	
	@Override
	public EndPoint getWrappingEndPoint(){
		return ep;
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("Physical Operator");
		sb.append(Utils.NL);
		sb.append("###############");
		sb.append(Utils.NL);
		sb.append("Name: "+this.name);
		sb.append(Utils.NL);
		sb.append("OpId: "+this.opId);
		sb.append(Utils.NL);
		sb.append("Stateful?: "+this.stateful);
		sb.append(Utils.NL);
		sb.append("SeepTask: "+this.seepTask.toString());
		sb.append(Utils.NL);
		sb.append("EndPoint: "+this.ep.toString());
		sb.append(Utils.NL);
		if(this.state != null){
			sb.append("SeepState: "+this.state.toString());
		}
		sb.append(Utils.NL);
		sb.append("#Downstream: "+this.downstreamConnections.size());
		sb.append(Utils.NL);
		for(int i = 0; i < this.downstreamConnections.size(); i++){
			DownstreamConnection down = downstreamConnections.get(i);
			int streamId = down.getStreamId();
			int downOpId = down.getDownstreamOperator().getOperatorId();
			sb.append("  Down-conn-"+i+"-> StreamId: "+streamId+" to opId: "
					+ ""+downOpId);
			sb.append(Utils.NL);
			if(down.getDownstreamOperator() instanceof SeepQueryPhysicalOperator){
				sb.append("EndPoint info: ");
				sb.append(((SeepQueryPhysicalOperator)down.getDownstreamOperator()).ep.toString());
				sb.append(Utils.NL);
			}
		}
		sb.append("#Upstream: "+this.upstreamConnections.size());
		sb.append(Utils.NL);
		for(int i = 0; i < this.upstreamConnections.size(); i++){
			UpstreamConnection up = upstreamConnections.get(i);
			sb.append("  Up-conn-"+i+"-> StreamId: "+up.getStreamId()+" to opId: "
					+ ""+up.getUpstreamOperator().getOperatorId()+""
							+ " with connType: "+up.getConnectionType()+" and dataOrigin: "+up.getDataOriginType());
			sb.append(Utils.NL);
		}
		return sb.toString();
	}
}
