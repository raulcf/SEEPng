package uk.ac.imperial.lsds.seep.api;

public class EvaluateResultsRuntimeEvent implements RuntimeEventType {

	private Object obj;
	
	public EvaluateResultsRuntimeEvent() { }
	
	public EvaluateResultsRuntimeEvent(Object obj) {
		this.obj = obj;
	}
	
	@Override
	public int type() {
		return RuntimeEventTypes.EVALUATE_RESULT.ofType();
	}
	
	public Object getEvaluateResults() {
		return obj;
	}

}
