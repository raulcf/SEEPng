package uk.ac.imperial.lsds.seep.core;

import java.util.Set;

public class DatasetMetadataPackage {

	public Set<DatasetMetadata> oldDatasets;
	public Set<DatasetMetadata> newDatasets;
	public Set<DatasetMetadata> usedDatasets;
	public double availableMemory;
	public long __time_freeDatasets;
	
	public DatasetMetadataPackage() { }
	
	public DatasetMetadataPackage(Set<DatasetMetadata> oldDatasets, Set<DatasetMetadata> newDatasets, Set<DatasetMetadata> usedDatasets, double availableMemory, long __time_freeDatasets) {
		this.oldDatasets = oldDatasets;
		this.newDatasets = newDatasets;
		this.usedDatasets = usedDatasets;
		this.availableMemory = availableMemory;
		this.__time_freeDatasets = __time_freeDatasets;
	}
}
