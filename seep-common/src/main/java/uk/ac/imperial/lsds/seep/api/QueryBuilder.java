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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.operator.LogicalOperator;
import uk.ac.imperial.lsds.seep.api.operator.SeepLogicalQuery;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.sinks.Sink;
import uk.ac.imperial.lsds.seep.api.operator.sources.Source;
import uk.ac.imperial.lsds.seep.api.state.SeepState;

public class QueryBuilder implements QueryAPI {
	
	private static SeepLogicalQuery qp = new SeepLogicalQuery();
	
	public SchemaBuilder schemaBuilder = SchemaBuilder.getInstance();
	public Set<Integer> usedIds = new HashSet<>();
	
	public static SeepLogicalQuery build(){
		// Check whether there are StaticSources, in which case, downstream to those become Sources
		for(LogicalOperator lo : qp.getAllOperators()) {
			for(UpstreamConnection uc : lo.upstreamConnections()) {
				if(uc.getUpstreamOperator() == null) {
					qp.addSource(lo); // the op with staticSource as upstream becomes Source
				}
			}
		}
		
		// Perform sanity checks
		if(qp.getSources().size() == 0){
			throw new InvalidQueryDefinitionException("The query must define at least one source");
		}
		if(qp.getSink() == null){
			throw new InvalidQueryDefinitionException("The query must define a sink");
		}
		return qp;
	}

	@Override
	public List<LogicalOperator> getQueryOperators() {
		return qp.getAllOperators();
	}

	@Override
	public List<SeepState> getQueryState() {
		return qp.getAllStates();
	}

	@Override
	public int getInitialPhysicalInstancesPerLogicalOperator(int logicalOperatorId) {
		return qp.getInitialPhysicalInstancesForLogicalOperator(logicalOperatorId);
	}

	@Override
	public List<LogicalOperator> getSources() {
		return qp.getSources();
	}

	@Override
	public LogicalOperator getSink() {
		return qp.getSink();
	}

	@Override
	public LogicalOperator newStatefulSource(Source seepTask, SeepState state, int opId) {
		if(usedIds.contains(opId)) {
			throw new InvalidQueryDefinitionException("OP id already used!");
		}
		usedIds.add(opId);
		return qp.newStatefulSource(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessSource(Source seepTask, int opId) {
		if(usedIds.contains(opId)) {
			throw new InvalidQueryDefinitionException("OP id already used!");
		}
		usedIds.add(opId);
		return qp.newStatelessSource(seepTask, opId);
	}

	@Override
	public LogicalOperator newStatefulOperator(SeepTask seepTask, SeepState state, int opId) {
		if(usedIds.contains(opId)) {
			throw new InvalidQueryDefinitionException("OP id already used!");
		}
		usedIds.add(opId);
		return qp.newStatefulOperator(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessOperator(SeepTask seepTask, int opId) {
		if(usedIds.contains(opId)) {
			throw new InvalidQueryDefinitionException("OP id already used!");
		}
		usedIds.add(opId);
		return qp.newStatelessOperator(seepTask, opId);
	}

	@Override
	public LogicalOperator newStatefulSink(Sink seepTask, SeepState state, int opId) {
		if(usedIds.contains(opId)) {
			throw new InvalidQueryDefinitionException("OP id already used!");
		}
		usedIds.add(opId);
		return qp.newStatefulSink(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessSink(Sink seepTask, int opId) {
		if(usedIds.contains(opId)) {
			throw new InvalidQueryDefinitionException("OP id already used!");
		}
		usedIds.add(opId);
		return qp.newStatelessSink(seepTask, opId);
	}

	@Override
	public void setInitialPhysicalInstancesForLogicalOperator(int opId,	int numInstances) {
		if(usedIds.contains(opId)) {
			throw new InvalidQueryDefinitionException("OP id already used!");
		}
		usedIds.add(opId);
		qp.setInitialPhysicalInstancesPerLogicalOperator(opId, numInstances);
	}

}
