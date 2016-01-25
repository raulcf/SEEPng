package uk.ac.imperial.lsds.seep.errors;

public class AttemptToReconcileStateNotInSnapshotModeException extends SeepException {
	
	private static final long serialVersionUID = 1L;

	public AttemptToReconcileStateNotInSnapshotModeException(String msg){
		super(msg);
	}
	
}
