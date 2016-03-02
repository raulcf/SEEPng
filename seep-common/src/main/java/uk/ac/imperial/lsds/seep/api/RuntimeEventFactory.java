package uk.ac.imperial.lsds.seep.api;

public class RuntimeEventFactory {

	public static RuntimeEvent makeSpillToDiskRuntimeEvent(int datasetId) {
		SpillToDiskRuntimeEvent stdre = new SpillToDiskRuntimeEvent(datasetId);
		RuntimeEvent re = new RuntimeEvent(stdre);
		return re;
	}

}
