package uk.ac.imperial.lsds.seep.core;

public class DatasetMetadata {

	private int datasetId;
	private long size;
	private boolean inMem;
	
	public DatasetMetadata() { }
	
	public DatasetMetadata(int datasetId, long size, boolean inMem) {
		this.datasetId = datasetId;
		this.size = size;
		this.inMem = inMem;
	}
	
	public int getDatasetId() {
		return datasetId;
	}
	
	public long getSize() {
		return size;
	}
	
	public boolean isInMem() {
		return inMem;
	}
	
}
