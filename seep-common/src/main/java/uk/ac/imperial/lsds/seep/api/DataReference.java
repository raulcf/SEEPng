package uk.ac.imperial.lsds.seep.api;

import java.rmi.server.UID;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public final class DataReference {
	
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
	 * Info necessary for the DRM to serve this DR
	 */
	private final DataStore dataStore;
	/**
	 * The endPoint to whom one should contact to request the DR
	 */
	private final EndPoint endPoint;
	
	private DataReference(int uid, DataStore dataStore, EndPoint endPoint, boolean managed, boolean partitioned, ServeMode serveMode) {
		this.ownerId = uid;
		this.dataStore = dataStore;
		this.endPoint = endPoint;
		this.managed = managed;
		this.partitioned = partitioned;
		this.serveMode = serveMode;
	}
	
	private DataReference(DataStore dataStore, EndPoint endPoint, boolean managed, boolean partitioned, ServeMode serveMode) {
		this.ownerId = new UID().hashCode();
		this.dataStore = dataStore;
		this.endPoint = endPoint;
		this.managed = managed;
		this.partitioned = partitioned;
		this.serveMode = serveMode;
	}
	
	public static DataReference makeManagedDataReferenceWithOwner(int ownerId, DataStore dataStore, EndPoint endPoint, ServeMode serveMode) {
		return new DataReference(ownerId, dataStore, endPoint, true, false, serveMode);
	}
	
	public static DataReference makeManagedAndPartitionedDataReferenceWithOwner(int ownerId, DataStore dataStore, EndPoint endPoint, ServeMode serveMode) {
		return new DataReference(ownerId, dataStore, endPoint, true, true, serveMode);
	}
	
	public static DataReference makeManagedDataReference(DataStore dataStore, EndPoint endPoint, ServeMode serveMode) {
		return new DataReference(dataStore, endPoint, true, false, serveMode);
	}
	
	public static DataReference makeManagedAndPartitionedDataReference(DataStore dataStore, EndPoint endPoint, ServeMode serveMode) {
		return new DataReference(dataStore, endPoint, true, true, serveMode);
	}
	
	public static DataReference makeExternalDataReference(DataStore dataStore) {
		return new DataReference(dataStore, null, false, false, null);
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
	
	public EndPoint getEndPoint() {
		return endPoint;
	}
	
	public ServeMode getServeMode() {
		return serveMode;
	}
	
	public boolean isPartitioned() {
		return partitioned;
	}
	
	public enum ServeMode {
		STREAM,
		STORE
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
