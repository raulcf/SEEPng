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


public class TagSink implements TaggingSink, Operator {

	final private static Logger LOG = LoggerFactory.getLogger(TagSink.class);
	
	private int id;
	
	private TagSink(int id) {
		this.id = id;
	}
	
	public static TagSink newSink(int id) {
		return new TagSink(id);
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
		return id;
	}

	@Override
	public String getOperatorName() {
		return "" + id + "";
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public SeepState getState() {
		return null;
	}

	@Override
	public SeepTask getSeepTask() {
		return null;
	}

	@Override
	public List<DownstreamConnection> downstreamConnections() {
		return null;
	}

	@Override
	public List<UpstreamConnection> upstreamConnections() {
		return null;
	}

}
