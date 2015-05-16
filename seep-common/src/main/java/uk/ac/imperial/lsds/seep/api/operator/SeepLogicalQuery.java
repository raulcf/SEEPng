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

package uk.ac.imperial.lsds.seep.api.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.InvalidQueryDefinitionException;
import uk.ac.imperial.lsds.seep.api.QueryExecutionMode;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.SeepState;


public class SeepLogicalQuery {
	
	final private static Logger LOG = LoggerFactory.getLogger(SeepLogicalQuery.class);
	
	private List<LogicalOperator> logicalOperators = new ArrayList<>();
	private List<LogicalOperator> sources = new ArrayList<>();
	// TODO: there could be many sinks
	private LogicalOperator sink;
	private List<SeepState> states = new ArrayList<>();
	private Map<Integer, Integer> initialPhysicalInstancesPerOperator = new HashMap<>();
	
	private QueryExecutionMode qem = QueryExecutionMode.ALL_MATERIALIZED;
	
	public void setExecutionModeHint(QueryExecutionMode qem) {
		if(this.validExecutionModeForThisQuery(qem)) {
			this.qem = qem;
		}
		else {
			LOG.warn("Impossible to honour the requested execution mode. Query will run with: "+qem.toString());
		}
		
	}
	
	public QueryExecutionMode getQueryExecutionMode() {
		return qem;
	}
	
	private boolean validExecutionModeForThisQuery(QueryExecutionMode qem){
		LOG.warn("We are not checking requested execution mode !!!");
		return true;
	}
	
	public List<LogicalOperator> getAllOperators(){
		return logicalOperators;
	}
	
	public LogicalOperator getOperatorWithId(int opId){
		for(LogicalOperator lo : logicalOperators){
			if(lo.getOperatorId() == opId)
				return lo;
		}
		return null;
	}
	
	public void cleanMarkerOperators(){
		Iterator<LogicalOperator> it = logicalOperators.iterator();
		while(it.hasNext()){
			Operator o = it.next();
			if(o.getSeepTask() instanceof Source){
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
	
	public List<LogicalOperator> getSources(){
		return sources;
	}
	
	public void addSource(LogicalOperator lo){
		this.sources.add(lo);
	}
	
	public LogicalOperator getSink(){
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
	
	public LogicalOperator newStatefulSource(Source seepTask, SeepState state, int opId){
		LogicalOperator lo = newStatefulOperator(seepTask, state, opId);
		this.sources.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatelessSource(Source seepTask, int opId){
		LogicalOperator lo = newStatelessOperator(seepTask, opId);
		this.sources.add(lo);
		return lo;
	}
	
	public LogicalOperator newStatefulOperator(SeepTask seepTask, SeepState state, int opId){
		state.setOwner(opId);
		LogicalOperator lo = SeepLogicalOperator.newStatefulOperator(opId, seepTask, state);
		logicalOperators.add(lo);
		states.add(state);
		return lo;
	}
	
	public LogicalOperator newStatelessOperator(SeepTask seepTask, int opId){
		LogicalOperator lo = SeepLogicalOperator.newStatelessOperator(opId, seepTask);
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
