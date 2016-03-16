package uk.ac.imperial.lsds.seep.api;

public class NotifyEndOfLoopRuntimeEvent implements RuntimeEventType {

	@Override
	public int type() {
		return RuntimeEventTypes.NOTIFY_END_LOOP.ofType();
	}

}
