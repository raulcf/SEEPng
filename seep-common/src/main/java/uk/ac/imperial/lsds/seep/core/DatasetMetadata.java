package uk.ac.imperial.lsds.seep.core;

public class DatasetMetadata {

	private int datasetId;
	private long size;
	private boolean inMem;
	private long creationCost;
	private int diskAccess;
	private int memAccess;
	
	public DatasetMetadata() { }
	
	public DatasetMetadata(int datasetId, long size, boolean inMem, long creationCost, int diskAccess, int memAccess) {
		this.datasetId = datasetId;
		this.size = size;
		this.inMem = inMem;
		this.creationCost = creationCost;
		this.diskAccess = diskAccess;
		this.memAccess = memAccess;
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
	
	public long getCreationCost() {
		return creationCost;
	}
	
	public int getDiskAccess() {
		return diskAccess;
	}
	
	public int getMemAccess() {
		return memAccess;
	}
	
	@Override
	public int hashCode() {
		return datasetId;
	}
	
	@Override
	public boolean equals(Object dm) {
		return dm.hashCode() == this.hashCode();
	}
	
}
