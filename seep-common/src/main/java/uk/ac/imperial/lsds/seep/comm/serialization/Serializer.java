package uk.ac.imperial.lsds.seep.comm.serialization;

public interface Serializer<T> {

	public byte[] serialize(T object);
	public T deserialize(byte[] data, Class<T> type);
	
}