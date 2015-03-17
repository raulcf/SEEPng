package uk.ac.imperial.lsds.seep.comm.serialization;

public enum SerializerType {

	NONE(0), 
	JAVA(1), 
	KRYO(2), 
	AVRO(3);
	
	private int id;
	
	SerializerType(int id){
		this.id = id;
	}
	
	public int ofType(){
		return id;
	}
	
}
