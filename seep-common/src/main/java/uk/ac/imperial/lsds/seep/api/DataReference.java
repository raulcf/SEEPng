package uk.ac.imperial.lsds.seep.api;

import java.rmi.server.UID;

import uk.ac.imperial.lsds.seep.infrastructure.ControlEndPoint;
import uk.ac.imperial.lsds.seep.infrastructure.SeepEndPoint;

public final class DataReference {
	
	public enum ServeMode {
		STREAM,	// When data is meant to be managed by seep in a different place that where it's generated
		STORE,	// When data is meant to be managed by seep where it is generated
		EMPTY,	// When the DataReference works as Marker and no data is associated to the DR
		SINK	// When data is not meant to be managed by seep, i.e. in the case of a sink that externalizes it
	}
	
	/**
	 * An ID for the DR. It can be used to include an "owner", e.g. the operator that can serve it.
	 */
	private final int ownerId;
	/**
	 * Whether the data is contained in SEEP or an external system
	 */
	private final boolean managed;
	/**
	 * Used to indicate how to dr is expected to be served 
	 */
	private final ServeMode serveMode;
	/**
	 * TODO: probably rename to shuffled
	 */
	private final boolean partitioned;
	/**
	 * When a DataReference is partitioned, this indicates the partitionId
	 * FIXME: make it final and see where to assign the id
	 */
	private int partitionId = -1;
	/**
	 * Info necessary for the DRM to serve this DR
	 */
	private final DataStore dataStore;
	/**
	 * The endPoint to whom one should contact to request the DR
	 */
	private final ControlEndPoint endPoint;
	
	private DataReference(int uid, DataStore dataStore, ControlEndPoint endPoint, boolean managed, boolean partitioned, ServeMode serveMode) {
		this.ownerId = uid;
		this.dataStore = dataStore;
		this.endPoint = endPoint;
		this.managed = managed;
		this.partitioned = partitioned;
		this.serveMode = serveMode;
	}
	
	private DataReference(DataStore dataStore, ControlEndPoint endPoint, boolean managed, boolean partitioned, ServeMode serveMode, int partitionId) {
		this.ownerId = new UID().hashCode();
		this.dataStore = dataStore;
		this.endPoint = endPoint;
		this.managed = managed;
		this.partitioned = partitioned;
		this.serveMode = serveMode;
		this.partitionId = partitionId;
	}
	
	private DataReference(ControlEndPoint endPoint) {
		this.ownerId = new UID().hashCode();
		this.dataStore = null;
		this.endPoint = endPoint;
		this.managed = true;
		this.partitioned = false;
		this.serveMode = ServeMode.STORE;
		this.partitionId = -1;
	}
	
    public static DataReference makeEmptyDataReference(ControlEndPoint endPoint) {
        return new DataReference(endPoint);
    }

	public static DataReference makeManagedDataReferenceWithOwner(int ownerId, DataStore dataStore, ControlEndPoint endPoint, ServeMode serveMode) {
		return new DataReference(ownerId, dataStore, endPoint, true, false, serveMode);
	}
	
	public static DataReference makeManagedAndPartitionedDataReferenceWithOwner(int ownerId, DataStore dataStore, ControlEndPoint endPoint, ServeMode serveMode) {
		return new DataReference(ownerId, dataStore, endPoint, true, true, serveMode);
	}
	
	public static DataReference makeManagedDataReference(DataStore dataStore, ControlEndPoint endPoint, ServeMode serveMode) {
		return new DataReference(dataStore, endPoint, true, false, serveMode, -1);
	}
	
	public static DataReference makeManagedAndPartitionedDataReference(DataStore dataStore, ControlEndPoint endPoint, ServeMode serveMode, int partitionId) {
		return new DataReference(dataStore, endPoint, true, true, serveMode, partitionId);
	}
	
	//TODO: consider refactoring name to makeSourceExternalDataReference, is there any other type of external dref?
	// TODO: not sure if hte previous comment still applies
	public static DataReference makeExternalDataReference(DataStore dataStore) {
		return new DataReference(dataStore, null, false, false, null, -1);
	}
	
	public static DataReference makeSinkExternalDataReference(DataStore dataStore) {
		return new DataReference(dataStore, null, false, false, ServeMode.SINK, -1);
	}
	
	public int getId() {
		return ownerId;
	}
	
	public boolean isManaged() {
		return managed;
	}
	
	public DataStore getDataStore() {
		return dataStore;
	}
	
	public SeepEndPoint getControlEndPoint() {
		return endPoint;
	}
	
	public ServeMode getServeMode() {
		return serveMode;
	}
	
	public boolean isPartitioned() {
		return partitioned;
	}
	
	public int getPartitionId() {
		return partitionId;
	}
	
	/**
	 * Empty constructor for Kryo serialization
	 */
	public DataReference() {
		this.ownerId = 0;
		this.managed = false;
		this.serveMode = null;
		this.partitioned = false;
		this.dataStore = null;
		this.endPoint = null;
	}

}
