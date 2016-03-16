package uk.ac.imperial.lsds.seep.api;

public class RuntimeEvent {

	private int type;
	
	private SpillToDiskRuntimeEvent stdre;
	private NotifyEndOfLoopRuntimeEvent nel;
	private EvaluateResultsRuntimeEvent eres;
	
	public RuntimeEvent() { }
	
	public RuntimeEvent(RuntimeEventType ret) {
		int type = ret.type();
		if(type == RuntimeEventTypes.DATASET_SPILL_TO_DISK.ofType()) {
			this.stdre = (SpillToDiskRuntimeEvent)ret;
		}
		else if(type == RuntimeEventTypes.NOTIFY_END_LOOP.ofType()) {
			this.nel = (NotifyEndOfLoopRuntimeEvent) ret;
		}
		else if(type == RuntimeEventTypes.EVALUATE_RESULT.ofType()) {
			this.eres = (EvaluateResultsRuntimeEvent) ret;
		}
	}
	
	public int type() {
		return type;
	}
	
	public SpillToDiskRuntimeEvent getSpillToDiskRuntimeEvent() {
		return stdre;
	}
	
	public NotifyEndOfLoopRuntimeEvent getNotifyEndOfLoopRuntimeEvent() {
		return nel;
	} 
	
	public EvaluateResultsRuntimeEvent getEvaluateResultsRuntimeEvent() {
		return eres;
	}
	
}
