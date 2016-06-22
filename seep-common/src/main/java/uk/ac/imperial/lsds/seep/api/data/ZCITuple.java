package uk.ac.imperial.lsds.seep.api.data;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.seep.errors.SchemaException;

public class ZCITuple extends ITuple {
	
	private ByteBuffer ptr;
	private int bufferPtrPosition;
	
	public ZCITuple(Schema schema) {
		super(schema);
	}
	
	public void assignBuffer(ByteBuffer ptr) {
		this.ptr = ptr;
	}
	
	public void setBufferPtr(int newPosition) {
		this.bufferPtrPosition = newPosition;
	}
	
	public int getInt(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.INT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.INT+"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getInt();
	}
	
	public int getInt(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getInt();
	}
	
	public long getLong(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.LONG)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.LONG +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getLong();
	}
	
	public long getLong(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getLong();
	}
	
	public float getFloat(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.FLOAT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.FLOAT +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getFloat();
	}
	
	public float getFloat(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getFloat();
	}
	
	public double getDouble(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.DOUBLE)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.DOUBLE +"' with name '"+fieldName+"'");
		}
		
		int offset = mapFieldToOffset.get(fieldName);
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getDouble();
	}
	
	public double getDouble(int idx) {
		int offset = mapIdxToOffset[idx];
		int ptrPosition = bufferPtrPosition + offset;
		ptr.position(ptrPosition);
		return ptr.getDouble();
	}
 
}
