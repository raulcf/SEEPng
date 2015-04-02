package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;

public class OTuple {

	private Schema schema;
	private Object[] values;
	private final int fixedSchemaSize;
	
	private byte[] data;
	
	public OTuple(Schema schema){
		this.schema = schema;
		this.values = new Object[schema.fields().length];
		if(! schema.isVariableSize()){
			// This only happens once
			this.fixedSchemaSize = this.calculateSizeFromSchema();
		}
		else {
			this.fixedSchemaSize = -1; // variable size, so it needs to be computed per tuple
		}
	}
	
	public byte[] getData(){
		return data;
	}
	
	public static byte[] create(Schema schema, String[] fields, Object[] vs) {
		OTuple o = new OTuple(schema);
		if(fields.length != vs.length){
			throw new SchemaException("Mismatch between fieldNames and values");
		}
		if(fields.length != schema.fields().length){
			throw new SchemaException("Mismatch between input fields and schema fields");
		}
		Object[] values = new Object[vs.length];
		for(int i = 0; i < fields.length; i++){
			Object toTypeCheck = vs[i];
			if(! schema.typeCheck(fields[i], toTypeCheck)){
				String error = "Field: "+fields[i].toString()+" does not type check";
				throw new SchemaException(error);
			}
			else{
				values[i] = toTypeCheck;
			}
		}
		o.values = values;
		return o.getBytes();
	}
	
	public static byte[] createUnsafe(Type[] types, Object[] values, int size){
		byte[] data = new byte[size];
		ByteBuffer wrapper = ByteBuffer.wrap(data);
		for(int i = 0; i < values.length; i++) {
			Type t = types[i];
			t.write(wrapper, values[i]);
		}
		return data;
	}
	
	public static byte[] createUnsafe(Type[] types, Object[] values){
		int size = calculateSizeFromTypes(types, values);
		return createUnsafe(types, values, size);
	}
	
	private byte[] getBytes(){
		int requiredSize = this.fixedSchemaSize;
		if(schema.isVariableSize()){
			requiredSize = calculateSizeFromSchema(); 
		}
		data = new byte[requiredSize];
		ByteBuffer wrapper = ByteBuffer.wrap(data);
		for(int i = 0; i < values.length; i++){
			Type t = schema.fields()[i];
			t.write(wrapper, values[i]);
		}
		return data;
	}
	
	private int calculateSizeFromSchema(){
		int size = 0;
		for(int i = 0; i < schema.fields().length; i++){
			Type t = schema.fields()[i];
			size = size + t.sizeOf(values[i]);
		}
		return size;
	}
	
	public static int calculateSizeFromTypes(Type[] types, Object[] values){
		int size = 0;
		for(int i = 0; i < types.length; i++){
			size = size + types[i].sizeOf(values[i]);
		}
		return size;
	}
	
}
