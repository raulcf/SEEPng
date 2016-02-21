package uk.ac.imperial.lsds.seep.api.operator.sinks;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.imperial.lsds.seep.api.ConnectionType;
import uk.ac.imperial.lsds.seep.api.DataStore;
import uk.ac.imperial.lsds.seep.api.SeepTask;
import uk.ac.imperial.lsds.seep.api.operator.DownstreamConnection;
import uk.ac.imperial.lsds.seep.api.operator.Operator;
import uk.ac.imperial.lsds.seep.api.operator.UpstreamConnection;
import uk.ac.imperial.lsds.seep.api.state.SeepState;


public class MarkerSink implements TaggingSink, Operator {

	final private static Logger LOG = LoggerFactory.getLogger(MarkerSink.class);
	
	private int id;
	
	private MarkerSink(int id) {
		this.id = id;
	}
	
	public static MarkerSink newSink(int id) {
		return new MarkerSink(id);
	}
	
	/**
	 * Operator interface: Ideally this API should not be exposed to users. This requires
	 * iface refactoring so that Connectable.connectTo takes something other than an operator
	 */

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			DataStore dataStore) {
		LOG.warn("A sink cannot be connected to other Connectable");
	}

	@Override
	public void connectTo(Operator downstreamOperator, int streamId,
			DataStore dataStore, ConnectionType connectionType) {
		LOG.warn("A sink cannot be connected to other Connectable");
	}

	@Override
	public int getOperatorId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getOperatorName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isStateful() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public SeepState getState() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SeepTask getSeepTask() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<DownstreamConnection> downstreamConnections() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<UpstreamConnection> upstreamConnections() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * 
	 * @see uk.ac.imperial.lsds.seep.api.operator.Operator#hasPriority()
	 */
	@Override
	public boolean hasPriority() {
		return false;
	}

	@Override
	public int getPriority() {
		return 0;
	}

	@Override
	public void setPriority(int p) {
		// TODO Auto-generated method stub
	}

}
