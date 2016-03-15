package uk.ac.imperial.lsds.seep.api;

public class RuntimeEventFactory {

	public static RuntimeEvent makeSpillToDiskRuntimeEvent(int datasetId) {
		SpillToDiskRuntimeEvent stdre = new SpillToDiskRuntimeEvent(datasetId);
		RuntimeEvent re = new RuntimeEvent(stdre);
		return re;
	}

	public static RuntimeEvent makeNotifyEndOfLoop() {
		NotifyEndOfLoopRuntimeEvent e = new NotifyEndOfLoopRuntimeEvent();
		RuntimeEvent re = new RuntimeEvent(e);
		return re;
	}

	public static RuntimeEvent makeEvaluateResults(Object obj) {
		EvaluateResultsRuntimeEvent e = new EvaluateResultsRuntimeEvent(obj);
		RuntimeEvent re = new RuntimeEvent(e);
		return re;
	}

}
