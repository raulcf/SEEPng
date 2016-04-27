package uk.ac.imperial.lsds.seep.core;

import java.util.Set;

public class DatasetMetadataPackage {

	public Set<DatasetMetadata> oldDatasets;
	public Set<DatasetMetadata> newDatasets;
	public Set<DatasetMetadata> usedDatasets;
	
	public DatasetMetadataPackage() { }
	
	public DatasetMetadataPackage(Set<DatasetMetadata> oldDatasets, Set<DatasetMetadata> newDatasets, Set<DatasetMetadata> usedDatasets) {
		this.oldDatasets = oldDatasets;
		this.newDatasets = newDatasets;
		this.usedDatasets = usedDatasets;
	}
}
