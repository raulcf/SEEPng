package uk.ac.imperial.lsds.seep.api;

import java.rmi.server.UID;

import uk.ac.imperial.lsds.seep.infrastructure.EndPoint;

public final class DataReference {

	private final int uid;
	private final boolean managed;
	private final ServeMode serveMode; 
	private final boolean partitioned;
	private final DataStore dataStore;
	private final EndPoint endPoint;
	
	private DataReference(int uid, DataStore dataStore, EndPoint endPoint, boolean managed, boolean partitioned, ServeMode serveMode) {
		this.uid = uid;
		this.dataStore = dataStore;
		this.endPoint = endPoint;
		this.managed = managed;
		this.partitioned = partitioned;
		this.serveMode = serveMode;
	}
	
	private DataReference(DataStore dataStore, EndPoint endPoint, boolean managed, boolean partitioned, ServeMode serveMode) {
		this.uid = new UID().hashCode();
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
	
	public static DataReference makeExternalDataReference(DataStore dataStore, EndPoint endPoint, ServeMode serveMode) {
		return new DataReference(dataStore, endPoint, false, false, serveMode);
	}
	
	public int getId() {
		return uid;
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

}
