/*******************************************************************************
 * Copyright (c) 2013 Imperial College London.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Raul Castro Fernandez - initial design and implementation
 ******************************************************************************/

package uk.ac.imperial.lsds.seep.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import uk.ac.imperial.lsds.seep.api.state.SeepState;


public class LogicalSeepQuery {
	
	private List<Operator> logicalOperators = new ArrayList<>();
	private List<Operator> sources = new ArrayList<>();
	private Operator sink;
	private List<SeepState> states = new ArrayList<>();
	private Map<Integer, Integer> initialPhysicalInstancesPerOperator = new HashMap<>();
	
	public List<Operator> getAllOperators(){
		return logicalOperators;
	}
	
	public Operator getOperatorWithId(int opId){
		for(Operator lo : logicalOperators){
			if(lo.getOperatorId() == opId)
				return lo;
		}
		return null;
	}
	
	public void cleanMarkerOperators(){
		Iterator<Operator> it = logicalOperators.iterator();
		while(it.hasNext()){
			Operator o = it.next();
			if(o.getSeepTask() instanceof Source||o.getSeepTask() instanceof Sink){
				it.remove();
			}
		}
		it = sources.iterator();
		while(it.hasNext()){
			Operator o = it.next();
			if(o.getSeepTask() instanceof Source){
				it.remove();
			}
		}
	}
	
	public List<SeepState> getAllStates(){
		return states;
	}
	
	public List<Operator> getSources(){
		return sources;
	}
	
	public void addSource(LogicalOperator lo){
		this.sources.add(lo);
	}
	
	public Operator getSink(){
		return sink;
	}
	
	public void setInitialPhysicalInstancesPerLogicalOperator(int opId, int numInstances) {
		// Sanity checks
		if(numInstances - 1 < 1){
			throw new InvalidQueryDefinitionException("Minimum num Instances per logicalOperator is 1");
		}
		if(this.initialPhysicalInstancesPerOperator.containsKey(opId)){
			throw new InvalidQueryDefinitionException("Illegal action. Can set up numInstances only once");
		}
		// Operator to statically scale out
		LogicalOperator lo = null;
		for(Operator o : logicalOperators){
			if(o.getOperatorId() == opId) lo = (LogicalOperator)o;
		}
		if(lo == null){
			throw new InvalidQueryDefinitionException("Impossible to set num instances for NON-EXISTENT op: "+opId);
		}
		// Create scale out and update numInstances per op
		for(int instance = 0; instance < (numInstances-1); instance++) { // with 1 instance, we don't need to do anything
			int instanceOpId = getNewOpIdForInstance(opId, instance);
			LogicalOperator newInstance = null;
			if(lo.isStateful()){
				newInstance = this.newStatefulOperator(lo.getSeepTask(), lo.getState(), instanceOpId);
			}
			else{
				newInstance = this.newStatelessOperator(lo.getSeepTask(), instanceOpId);
			}
			connectInstance(lo, newInstance);
		}
		this.initialPhysicalInstancesPerOperator.put(opId, numInstances);
	}
	
	private void connectInstance(LogicalOperator original, LogicalOperator newInstance){
		for(DownstreamConnection dc : original.downstreamConnections()){
			newInstance.connectTo(dc.getDownstreamOperator(), dc.getStreamId(), dc.getSchema(), dc.getConnectionType(), dc.getExpectedDataOriginOfDownstream());
		}
		for(UpstreamConnection uc : original.upstreamConnections()){
			uc.getUpstreamOperator().connectTo(newInstance, uc.getStreamId(), uc.getExpectedSchema(), uc.getConnectionType(), uc.getDataOrigin());
		}
	}
		
	// TODO: there are better ways to do this...
	private int getNewOpIdForInstance(int opId, int it){
		return opId * 1000 + it;
	}
	
	public int getInitialPhysicalInstancesForLogicalOperator(int opId){
		if (initialPhysicalInstancesPerOperator.containsKey(opId))
			return initialPhysicalInstancesPerOperator.get(opId);
		else
			return 1; // there is always, at least one instance per defined operator
	}
	
	public boolean hasSetInitialPhysicalInstances(int opId){
		return initialPhysicalInstancesPerOperator.containsKey(opId);
	}
	
	public LogicalOperator newStatefulSource(SeepTask seepTask, SeepState state, int opId){
		LogicalOperator lo = newStatefulOperator(seepTask, state, opId);
		this.sources.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatelessSource(SeepTask seepTask, int opId){
		LogicalOperator lo = newStatelessOperator(seepTask, opId);
		this.sources.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatefulOperator(SeepTask seepTask, SeepState state, int opId){
		state.setOwner(opId);
		LogicalOperator lo = SeepQueryLogicalOperator.newStatefulOperator(opId, seepTask, state);
		logicalOperators.add(lo);
		states.add(state);
		return lo;
	}
	
	public LogicalOperator newStatelessOperator(SeepTask seepTask, int opId){
		LogicalOperator lo = SeepQueryLogicalOperator.newStatelessOperator(opId, seepTask);
		logicalOperators.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatefulSink(SeepTask seepTask, SeepState state, int opId){
		LogicalOperator lo = newStatefulOperator(seepTask, state, opId);
		this.sink = lo;
		return lo;
	}
	
	public LogicalOperator newStatelessSink(SeepTask seepTask, int opId){
		LogicalOperator lo = newStatelessOperator(seepTask, opId);
		this.sink = lo;
		return lo;
	}
	
	@Override
	public String toString(){
		String ls = System.getProperty("line.separator");
		StringBuilder sb = new StringBuilder();
		sb.append("Seep Query");
		sb.append(ls);
		sb.append("##########");
		sb.append(ls);
		sb.append("#Sources: "+this.sources.size());
		sb.append(ls);
		sb.append("#Operators(including-sources): "+this.logicalOperators.size());
		sb.append(ls);
		sb.append("#States: "+this.states.size());
		sb.append(ls);
		return sb.toString();
	}
}
