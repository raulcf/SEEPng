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

import java.util.List;

import uk.ac.imperial.lsds.seep.api.data.Schema.SchemaBuilder;
import uk.ac.imperial.lsds.seep.api.state.SeepState;

public class QueryBuilder implements QueryAPI {
	
	private static LogicalSeepQuery qp = new LogicalSeepQuery();
	
	public SchemaBuilder schemaBuilder = SchemaBuilder.getInstance();
	
	public static LogicalSeepQuery build(){
		// Check nonOperator sources and adjust sources accordingly
		for(Operator o : qp.getAllOperators()){
			for(UpstreamConnection uc : o.upstreamConnections()){
				LogicalOperator lo = (LogicalOperator) uc.getUpstreamOperator();
				if(lo.getSeepTask() instanceof Source){
					// This operator becomes a source
					qp.addSource((LogicalOperator)o);
				}
			}
		}
		// Remove the template source
		qp.cleanMarkerOperators();
		
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
	public List<Operator> getQueryOperators() {
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
	public List<Operator> getSources() {
		return qp.getSources();
	}

	@Override
	public Operator getSink() {
		return qp.getSink();
	}

	@Override
	public LogicalOperator newStatefulSource(SeepTask seepTask,	SeepState state, int opId) {
		return qp.newStatefulSource(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessSource(SeepTask seepTask, int opId) {
		return qp.newStatelessSource(seepTask, opId);
	}

	@Override
	public LogicalOperator newStatefulOperator(SeepTask seepTask, SeepState state, int opId) {
		return qp.newStatefulOperator(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessOperator(SeepTask seepTask, int opId) {
		return qp.newStatelessOperator(seepTask, opId);
	}

	@Override
	public LogicalOperator newStatefulSink(SeepTask seepTask, SeepState state, int opId) {
		return qp.newStatefulSink(seepTask, state, opId);
	}

	@Override
	public LogicalOperator newStatelessSink(SeepTask seepTask, int opId) {
		return qp.newStatelessSink(seepTask, opId);
	}

	@Override
	public void setInitialPhysicalInstancesForLogicalOperator(int opId,	int numInstances) {
		qp.setInitialPhysicalInstancesPerLogicalOperator(opId, numInstances);
	}

}
