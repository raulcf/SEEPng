package uk.ac.imperial.lsds.seep.api.data;

import java.util.HashMap;
import java.util.Map;

import uk.ac.imperial.lsds.seep.errors.SchemaException;

public class TransporterITuple extends ITuple {

	private Object[] values;
	private Map<String, Integer> fieldToIdx;
	private Map<Integer, Integer> offsetToIdx;
		
	public TransporterITuple(Schema schema) {
		super(schema);
		fieldToIdx = new HashMap<>();
		offsetToIdx = new HashMap<>();
		populateOffsets();
	}
	
	public void setValues(Object[] values) {
		this.values = values;
	}
	
	public int getInt(String fieldName) {
		if(! schema.hasField(fieldName)) {
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.INT)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.INT+"' with name '"+fieldName+"'");
		}
		
		int idx = fieldToIdx.get(fieldName);
		return (Integer)values[idx];
	}
	
	public int getInt(int idx) {
		int i = offsetToIdx.get(idx);
		return (Integer)values[i];
	}
	
	public long getLong(String fieldName){
		if(! schema.hasField(fieldName)){
			throw new SchemaException("Current Schema does not have a field with name '"+fieldName+ "'");
		}
		if(! schema.typeCheck(fieldName, Type.LONG)) {
			throw new SchemaException("Current Schema cannot typeCheck a field type '"+ Type.LONG +"' with name '"+fieldName+"'");
		}
		
		int idx = fieldToIdx.get(fieldName);
		return (Long)values[idx];
	}
	
	public long getLong(int idx) {
		int i = offsetToIdx.get(idx);
		return (Long)values[i];
	}
	
	private void populateOffsets(){
		Type[] fields = schema.fields();
		String[] names = schema.names();
		int offset = 0;
		for(int i = 0; i < fields.length; i++){
			Type t = fields[i];
			fieldToIdx.put(names[i], i);
			offsetToIdx.put(offset, i);
			if(! t.isVariableSize()){
				// if not variable we just get the size of the Type
				offset = offset + t.sizeOf(null);
			}
		}
	}

}
